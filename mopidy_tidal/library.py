from __future__ import unicode_literals

import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Union

from mopidy import backend, models
from mopidy.models import Image, Ref, SearchResult, Track
from requests.exceptions import HTTPError
from tidalapi.exceptions import ObjectNotFound, TooManyRequests

from mopidy_tidal import full_models_mappers, ref_models_mappers
from mopidy_tidal.login_hack import login_hack
from mopidy_tidal.lru_cache import LruCache
from mopidy_tidal.playlists import PlaylistMetadataCache
from mopidy_tidal.utils import apply_watermark

if TYPE_CHECKING:  # pragma: no cover
    from mopidy_tidal.backend import TidalBackend

logger = logging.getLogger(__name__)


class ImagesGetter:
    def __init__(self, session):
        self._session = session
        self._image_cache = LruCache(directory="image")

    @staticmethod
    def _log_image_not_found(obj):
        logger.debug(
            'No images available for %s "%s"',
            type(obj).__name__,
            getattr(obj, "name", getattr(obj, "title", getattr(obj, "id"))),
        )

    @classmethod
    def _get_image_uri(cls, obj):
        method = None

        if hasattr(obj, "image"):
            if hasattr(obj, "picture") and getattr(obj, "picture", None) is not None:
                method = obj.image
            elif (
                hasattr(obj, "square_picture")
                and getattr(obj, "square_picture", None) is not None
            ):
                method = obj.image
            elif hasattr(obj, "cover") and getattr(obj, "cover", None) is not None:
                method = obj.image
            elif hasattr(obj, "images") and getattr(obj, "images", None) is not None:
                # Mix types contain images type with three small/medium/large image sizes
                method = obj.image
            else:
                # Handle artists/albums/playlists/mixes with missing images
                cls._log_image_not_found(obj)
                return
        else:
            cls._log_image_not_found(obj)
            return

        dimensions = (750, 640, 480)
        for dim in dimensions:
            args = (dim,)
            try:
                return method(*args)
            except ValueError:
                pass

        cls._log_image_not_found(obj)

    def _get_api_getter(self, item_type: str):
        return getattr(self._session, item_type, None)

    def _get_images(self, uri) -> List[Image]:
        assert uri.startswith("tidal:"), f"Invalid TIDAL URI: {uri}"

        parts = uri.split(":")
        item_type = parts[1]
        if item_type == "track":
            # For tracks, retrieve the artwork of the associated album
            item_type = "album"
            item_id = parts[3]
            uri = ":".join([parts[0], "album", parts[3]])
        elif item_type == "album":
            item_id = parts[2]
        elif item_type == "playlist":
            item_id = parts[2]
        elif item_type == "artist":
            item_id = parts[2]
        elif item_type == "mix":
            item_id = parts[2]
        else:
            # uri has no image associated to it (eg. tidal:mood tidal:genres etc.)
            return []

        if uri in self._image_cache:
            # Cache hit
            logger.debug("Cache hit for {}".format(uri))
            return self._image_cache[uri]

        logger.debug("Retrieving %r from the API", uri)
        getter = self._get_api_getter(item_type)
        if not getter:
            logger.warning("The API item type %s has no session getters", item_type)
            return []

        item = getter(item_id)
        if not item:
            logger.debug("%r is not available on the backend", uri)
            return []

        img_uri = self._get_image_uri(item)
        if not img_uri:
            logger.debug("%r has no associated images", uri)
            return []

        logger.debug("Image URL for %r: %r", uri, img_uri)
        return [Image(uri=img_uri, width=320, height=320)]

    def __call__(self, uri: str) -> Tuple[str, List[Image]]:
        parts = uri.split(":")
        item_type = parts[1]
        if item_type not in ["artist", "album", "playlist", "mix", "track"]:
            logger.debug("URI %s type has no image getters", uri)
            return uri, []
        try:
            return uri, self._get_images(uri)
        except (AssertionError, AttributeError, ObjectNotFound) as err:
            logger.error("%s when processing URI %r: %s", type(err), uri, err)
            return uri, []
        except (HTTPError, TooManyRequests) as err:
            logger.error("%s when processing URI %r: %s", type(err), uri, err)
            return uri, []

    def cache_update(self, images):
        self._image_cache.update(images)


class TidalLibraryProvider(backend.LibraryProvider):
    root_directory = models.Ref.directory(uri="tidal:directory", name="Tidal")
    backend: "TidalBackend"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._artist_cache = LruCache()
        self._album_cache = LruCache()
        self._track_cache = LruCache()
        self._playlist_cache = PlaylistMetadataCache()

    @property
    def _session(self):
        return self.backend.session

    @staticmethod
    def _convert_tracks(
        tracks: Union[
            dict, list[dict], list[Track], list[tuple[str, Union[Any, None]]]
        ],
    ) -> list[Track]:
        """
        Convert looked up tracks to a list of Track objects.

        This is to ensure compatibility between Mopidy < 4.0 (which expects a
        list of Track objects) and Mopidy >= 4.0 (which expects a list of key-value
        tuples or a dictionary).
        """
        if isinstance(tracks, list) and all(isinstance(t, Track) for t in tracks):
            return tracks  # type: ignore[return-value]
        if isinstance(tracks, dict):
            # Single-track lookup returns a dictionary
            tracks = [tracks]  # type: ignore[assignment]
        if isinstance(tracks, list) and all(isinstance(t, tuple) for t in tracks):
            # Single-track lookup returns a list of tuples that need to be converted
            # to a dictionary first
            tracks = [dict(tracks)]  # type: ignore[assignment]
        if isinstance(tracks, list) and all(isinstance(t, dict) for t in tracks):
            return [
                Track(
                    **{
                        k: t.get(k)  # type: ignore[assignment]
                        for k in Track.model_fields  # pylint: disable=no-member
                        if k != "model"
                    },
                )
                for t in tracks
            ]

        raise TypeError(
            f"Unsupported track format: {type(tracks)}. "
            "Expected list of Track objects, list of tuples, or a dictionary."
        )

    @login_hack(passthrough=True)
    def get_distinct(self, field, query=None) -> set[str]:
        from mopidy_tidal.search import tidal_search

        logger.debug("Browsing distinct %s with query %r", field, query)
        session = self._session

        if not query:  # library root
            if field in {"artist", "albumartist"}:
                return {
                    apply_watermark(a.name) for a in session.user.favorites.artists()
                }
            elif field == "album":
                return {
                    apply_watermark(a.name) for a in session.user.favorites.albums()
                }
            elif field in {"track", "track_name"}:
                return {
                    apply_watermark(t.name) for t in session.user.favorites.tracks()
                }
        else:
            if field == "artist":
                return {
                    apply_watermark(a.name) for a in session.user.favorites.artists()
                }
            elif field in {"album", "albumartist"}:
                artists, _, _ = tidal_search(session, query=query, exact=True)
                if len(artists) > 0:
                    artist = artists[0]
                    artist_id = artist.uri.split(":")[2]
                    return {
                        apply_watermark(a.name)
                        for a in self._get_artist_albums(session, artist_id)
                    }
            elif field in {"track", "track_name"}:
                return {
                    apply_watermark(t.name) for t in session.user.favorites.tracks()
                }
            pass

        return set()

    @login_hack
    def browse(self, uri) -> list[Ref]:
        logger.info("Browsing uri %s", uri)
        if not uri or not uri.startswith("tidal:"):
            return []

        session = self._session

        # summaries

        if uri == self.root_directory.uri:
            return ref_models_mappers.create_root()

        elif uri == "tidal:my_artists":
            return ref_models_mappers.create_artists(
                session.user.favorites.artists_paginated()
            )
        elif uri == "tidal:my_albums":
            return ref_models_mappers.create_albums(
                session.user.favorites.albums_paginated()
            )
        elif uri == "tidal:my_playlists":
            return self.backend.playlists.as_list()
        elif uri == "tidal:my_mixes":
            return ref_models_mappers.create_mixes(session.user.favorites.mixes())
        elif uri == "tidal:my_tracks":
            return ref_models_mappers.create_tracks(
                session.user.favorites.tracks_paginated()
            )
        elif uri == "tidal:home":
            return ref_models_mappers.create_category_directories(
                uri, session.home(use_legacy_endpoint=True)
            )
        elif uri == "tidal:for_you":
            return ref_models_mappers.create_category_directories(
                uri, session.for_you()
            )
        elif uri == "tidal:explore":
            return ref_models_mappers.create_category_directories(
                uri, session.explore()
            )
        elif uri == "tidal:hires":
            return ref_models_mappers.create_category_directories(
                uri, session.hires_page()
            )
        elif uri == "tidal:moods":
            return ref_models_mappers.create_moods(session.moods())
        elif uri == "tidal:mixes":
            return ref_models_mappers.create_mixes([m for m in session.mixes()])
        elif uri == "tidal:genres":
            return ref_models_mappers.create_genres(session.genre.get_genres())

        # Category nested on a page (eg. page(For You).category[0..n])
        # These have 3-part uris
        with suppress(ValueError):
            _, page_id, type, category_id = uri.split(":")
            category = session.page.get(f"pages/{page_id}").categories[int(category_id)]
            return ref_models_mappers.create_mixed_directory(category.items)

        # details with 2-part uris
        try:
            _, type, id = uri.split(":")

            if type == "album":
                return ref_models_mappers.create_tracks(
                    self._get_album_tracks(session, id)
                )

            elif type == "artist":
                top_10_tracks = ref_models_mappers.create_tracks(
                    self._get_artist_top_tracks(session, id)[:10]
                )

                albums = ref_models_mappers.create_albums(
                    self._get_artist_albums(session, id)
                )

                return albums + top_10_tracks

            elif type == "playlist":
                return ref_models_mappers.create_tracks(
                    self._get_playlist_tracks(session, id)
                )

            elif type == "mood":
                return ref_models_mappers.create_playlists(
                    self._get_mood_items(session, id)
                )

            elif type == "genre":
                return ref_models_mappers.create_playlists(
                    self._get_genre_items(session, id)
                )

            elif type == "mix":
                return ref_models_mappers.create_tracks(
                    self._get_mix_tracks(session, id)
                )

            elif type == "page":
                return ref_models_mappers.create_mixed_directory(session.page.get(id))
            else:
                return []

        except ValueError:
            logger.exception("Unable to parse uri '%s' for browse.", uri)
            return []
        except HTTPError:
            logger.exception("Unable to retrieve object from uri '%s'", uri)
            return []

    @login_hack
    def search(self, query=None, uris=None, exact=False) -> Optional[SearchResult]:
        from mopidy_tidal.search import tidal_search

        try:
            artists, albums, tracks = tidal_search(
                self._session, query=query, exact=exact
            )
            return SearchResult(artists=artists, albums=albums, tracks=tracks)
        except Exception as ex:
            logger.info("EX")
            logger.info("%r", ex)

    @login_hack
    def get_images(self, uris) -> dict[str, list[Image]]:
        logger.info("Searching Tidal for images for %r" % uris)
        images_getter = ImagesGetter(self._session)

        with ThreadPoolExecutor(4, thread_name_prefix="mopidy-tidal-images-") as pool:
            pool_res = pool.map(images_getter, uris)

        images = {uri: item_images for uri, item_images in pool_res if item_images}
        images_getter.cache_update(images)
        return images

    @login_hack
    def lookup(self, uris=None) -> list[Track]:
        if isinstance(uris, str):
            uris = [uris]
        if not hasattr(uris, "__iter__"):
            uris = [uris]

        tracks = []
        cache_updates = {}

        for uri in uris or []:
            data = []
            try:
                parts = uri.split(":")
                item_type = parts[1]
                cache_name = f"_{parts[1]}_cache"
                cache_miss = True

                try:
                    data = getattr(self, cache_name)[uri]
                    cache_miss = not bool(data)
                except (AttributeError, KeyError):
                    pass

                if cache_miss:
                    try:
                        lookup = getattr(self, f"_lookup_{parts[1]}")
                    except AttributeError:
                        continue

                    data = cache_data = lookup(self._session, parts)
                    cache_updates[cache_name] = cache_updates.get(cache_name, {})
                    if item_type == "playlist":
                        # Playlists should be persisted on the cache as objects,
                        # not as lists of tracks. Therefore, _lookup_playlist
                        # returns a tuple that we need to unpack
                        data, cache_data = data

                    cache_updates[cache_name][uri] = cache_data

                if item_type == "playlist" and not cache_miss:
                    tracks += data.tracks
                else:
                    tracks += data if hasattr(data, "__iter__") else [data]
            except HTTPError as err:
                logger.error("%s when processing URI %r: %s", type(err), uri, err)

        for cache_name, new_data in cache_updates.items():
            getattr(self, cache_name).update(new_data)

        tracks = self._convert_tracks(tracks)
        self._track_cache.update({track.uri: track for track in tracks})
        logger.info("Returning %d tracks", len(tracks))
        return tracks

    @classmethod
    def _get_playlist_tracks(cls, session, playlist_id):
        try:
            pl = session.playlist(playlist_id)
        except ObjectNotFound:
            logger.debug("No such playlist: %s", playlist_id)
            return []
        return pl.tracks_paginated()

    @staticmethod
    def _get_genre_items(session, genre_id):
        from tidalapi.playlist import Playlist

        filtered_genres = [g for g in session.genre.get_genres() if genre_id == g.path]
        if filtered_genres:
            return filtered_genres[0].items(Playlist)
        return []

    @staticmethod
    def _get_mood_items(session, mood_id):
        filtered_moods = [
            m for m in session.moods() if mood_id == m.api_path.split("/")[-1]
        ]

        if filtered_moods:
            mood = filtered_moods[0].get()
            return [p for p in mood]
        return []

    @staticmethod
    def _get_mix_tracks(session, mix_id):
        try:
            mix = session.mix(mix_id)
        except ObjectNotFound:
            logger.debug("No such mix: %s", mix_id)
            return []
        return mix.items()

    def _lookup_playlist(self, session, parts):
        playlist_id = parts[2]
        tidal_playlist = session.playlist(playlist_id)
        tidal_tracks = self._get_playlist_tracks(session, playlist_id)
        pl_tracks = full_models_mappers.create_mopidy_tracks(tidal_tracks)
        pl = full_models_mappers.create_mopidy_playlist(tidal_playlist, pl_tracks)
        # We need both the list of tracks and the mapped playlist object for
        # caching purposes
        return pl_tracks, pl

    @staticmethod
    def _get_artist_albums(session, artist_id):
        try:
            artist = session.artist(artist_id)
        except ObjectNotFound:
            logger.debug("No such artist: %s", artist_id)
            return []
        return artist.get_albums()

    @staticmethod
    def _get_album_tracks(session, album_id):
        try:
            album = session.album(album_id)
        except ObjectNotFound:
            logger.debug("No such album: %s", album_id)
            return []
        return album.tracks()

    def _lookup_track(self, session, parts):
        if len(parts) == 3:  # Track in format `tidal:track:<track_id>`
            track_id = parts[2]
            track = session.track(track_id)
            album_id = str(track.album.id)
        else:  # Track in format `tidal:track:<artist_id>:<album_id>:<track_id>`
            album_id = parts[3]
            track_id = parts[4]

        try:
            tracks = self._get_album_tracks(session, album_id)
        except ObjectNotFound:
            logger.warning("No such album: %s", album_id)
            tracks = []
        except TooManyRequests:
            logger.warning("TooManyRequests when fetching album tracks: %s", album_id)
            tracks = []

        # If album is unavailable, no tracks will be returned
        if tracks:
            track = next((t for t in tracks if t.id == int(track_id)), None)
            if not track:
                # A StopIteration is rare here, but it can happen - e.g. if the
                # cache is stale and the track has been removed from Tidal
                logger.warning("No such track: %s", track_id)  # pragma: no cover
                return []  # Return early to prevent accessing attributes of None

            return [full_models_mappers.create_mopidy_track(None, None, track)]
        else:
            return []

    def _lookup_album(self, session, parts):
        album_id = parts[2]
        tracks = self._get_album_tracks(session, album_id)

        return full_models_mappers.create_mopidy_tracks(tracks)

    @staticmethod
    def _get_artist_top_tracks(session, artist_id):
        return session.artist(artist_id).get_top_tracks()

    def _lookup_artist(self, session, parts):
        artist_id = parts[2]
        tracks = self._get_artist_top_tracks(session, artist_id)
        return full_models_mappers.create_mopidy_tracks(tracks)
