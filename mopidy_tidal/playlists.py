from __future__ import unicode_literals

import difflib
import logging
import operator
from pathlib import Path
from threading import Event, Timer
from typing import TYPE_CHECKING, Collection, List, Optional, Tuple, Union

from mopidy import backend
from mopidy.models import Playlist as MopidyPlaylist
from mopidy.models import Ref
from requests import HTTPError
from tidalapi.playlist import Playlist as TidalPlaylist

from mopidy_tidal import full_models_mappers
from mopidy_tidal.full_models_mappers import create_mopidy_playlist
from mopidy_tidal.helpers import to_timestamp
from mopidy_tidal.login_hack import login_hack
from mopidy_tidal.lru_cache import LruCache
from mopidy_tidal.utils import mock_track

if TYPE_CHECKING:  # pragma: no cover
    from mopidy_tidal.backend import TidalBackend

logger = logging.getLogger(__name__)


class PlaylistCache(LruCache):
    def __getitem__(
        self, key: Union[str, TidalPlaylist], *args, **kwargs
    ) -> MopidyPlaylist:
        uri = key.id if isinstance(key, TidalPlaylist) else key
        assert uri
        uri = f"tidal:playlist:{uri}" if not uri.startswith("tidal:playlist:") else uri

        playlist = super().__getitem__(uri, *args, **kwargs)
        if (
            playlist
            and isinstance(key, TidalPlaylist)
            and to_timestamp(key.last_updated) > to_timestamp(playlist.last_modified)
        ):
            # The playlist has been updated since last time:
            # we should refresh the associated cache entry
            logger.info('The playlist "%s" has been updated: refresh forced', key.name)

            raise KeyError(uri)

        return playlist


class PlaylistMetadataCache(PlaylistCache):
    def cache_file(self, key: str) -> Path:
        return super().cache_file(key, Path("playlist_metadata"))


class TidalPlaylistsProvider(backend.PlaylistsProvider):
    backend: "TidalBackend"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._playlists_metadata = PlaylistMetadataCache()
        self._playlists = PlaylistCache()
        self._current_tidal_playlists = []
        self._playlists_loaded_event = Event()

    def _calculate_added_and_removed_playlist_ids(
        self,
    ) -> Tuple[Collection[str], Collection[str]]:
        logger.info("Calculating playlist updates..")
        session = self.backend.session

        updated_playlists = session.user.favorites.playlists_paginated()

        self._current_tidal_playlists = updated_playlists
        updated_ids = set(pl.id for pl in updated_playlists)

        if not self._playlists_metadata:
            return updated_ids, set()

        current_ids = set(uri.split(":")[-1] for uri in self._playlists_metadata.keys())
        added_ids = updated_ids.difference(current_ids)
        removed_ids = current_ids.difference(updated_ids)

        self._playlists_metadata.prune(
            *[
                uri
                for uri in self._playlists_metadata.keys()
                if uri.split(":")[-1] in removed_ids
            ]
        )

        return added_ids, removed_ids

    def _has_changes(self, playlist: MopidyPlaylist):
        upstream_playlist = self.backend.session.playlist(playlist.uri.split(":")[-1])
        if not upstream_playlist:
            return True

        upstream_last_updated_at = to_timestamp(
            getattr(upstream_playlist, "last_updated", None)
        )
        local_last_updated_at = to_timestamp(playlist.last_modified)

        if not upstream_last_updated_at:
            logger.warning(
                "You are using a version of python-tidal that does not "
                "support last_updated on playlist objects"
            )
            return True

        if upstream_last_updated_at > local_last_updated_at:
            logger.info(
                'The playlist "%s" has been updated: refresh forced', playlist.name
            )
            return True

        return False

    @login_hack(list[Ref.playlist])
    def as_list(self) -> list[Ref]:
        if not self._playlists_loaded_event.is_set():
            added_ids, _ = self._calculate_added_and_removed_playlist_ids()
            if added_ids:
                self.refresh(include_items=False)

        logger.debug("Listing TIDAL playlists..")
        refs = [
            Ref.playlist(uri=pl.uri, name=pl.name)
            for pl in self._playlists_metadata.values()
        ]

        return sorted(refs, key=operator.attrgetter("name"))

    def _lookup_mix(self, uri):
        mix_id = uri.split(":")[-1]
        session = self.backend.session
        return session.mix(mix_id)

    def _get_or_refresh_playlist(self, uri) -> Optional[MopidyPlaylist]:
        parts = uri.split(":")
        if parts[1] == "mix":
            mix = self._lookup_mix(uri)
            return full_models_mappers.create_mopidy_mix_playlist(mix)

        playlist = self._playlists.get(uri)
        if (playlist is None) or (playlist and self._has_changes(playlist)):
            self.refresh(uri, include_items=True)
        return self._playlists.get(uri)

    def create(self, name):
        tidal_playlist = self.backend.session.user.create_playlist(name, "")
        pl = create_mopidy_playlist(tidal_playlist, [])

        self._current_tidal_playlists.append(tidal_playlist)
        self.refresh(pl.uri)
        return pl

    def delete(self, uri):
        playlist_id = uri.split(":")[-1]
        session = self.backend.session

        try:
            session.request.request(
                "DELETE",
                "playlists/{playlist_id}".format(
                    playlist_id=playlist_id,
                ),
            )
        except HTTPError as e:
            # If we got a 401, it's likely that the user is following
            # this playlist but they don't have permissions for removing
            # it. If that's the case, remove the playlist from the
            # favourites instead of deleting it.
            if e.response.status_code == 401 and uri in {
                f"tidal:playlist:{pl.id}"
                for pl in session.user.favorites.playlists_paginated()
            }:
                session.user.favorites.remove_playlist(playlist_id)
            else:
                raise e

        self._playlists_metadata.prune(uri)
        self._playlists.prune(uri)

    @login_hack
    def lookup(self, uri) -> Optional[MopidyPlaylist]:
        return self._get_or_refresh_playlist(uri)

    @login_hack
    def refresh(self, *uris, include_items: bool = True) -> dict[str, MopidyPlaylist]:
        if uris:
            logger.info("Looking up playlists: %r", uris)
        else:
            logger.info("Refreshing TIDAL playlists..")

        session = self.backend.session
        plists = self._current_tidal_playlists
        mapped_playlists = {}
        playlist_cache = self._playlists if include_items else self._playlists_metadata

        for pl in plists:
            uri = "tidal:playlist:" + pl.id
            # Skip or cache hit case
            if (uris and uri not in uris) or pl in playlist_cache:
                continue

            # Cache miss case
            if include_items:
                pl_tracks = self._retrieve_api_tracks(pl)
                tracks = full_models_mappers.create_mopidy_tracks(pl_tracks)
            else:
                # Create as many mock tracks as the number of items in the playlist.
                # Playlist metadata is concerned only with the number of tracks, not
                # the actual list.
                tracks = [mock_track] * pl.num_tracks

            mapped_playlists[uri] = MopidyPlaylist(
                uri=uri,
                name=pl.name,
                tracks=tracks,
                last_modified=to_timestamp(pl.last_updated),
            )

        # When we trigger a playlists_loaded event the backend may call as_list
        # again. Set an event in playlist_cache_refresh_secs seconds to ensure
        # that we don't perform another playlist sync.
        self._playlists_loaded_event.set()
        playlist_cache_refresh_secs = self.backend._config["tidal"].get(
            "playlist_cache_refresh_secs"
        )

        if playlist_cache_refresh_secs:
            Timer(
                playlist_cache_refresh_secs,
                lambda: self._playlists_loaded_event.clear(),
            ).start()

        # Update the right playlist cache and send the playlists_loaded event.
        playlist_cache.update(mapped_playlists)
        backend.BackendListener.send("playlists_loaded")
        logger.info("TIDAL playlists refreshed")

    @login_hack
    def get_items(self, uri) -> Optional[List[Ref]]:
        playlist = self._get_or_refresh_playlist(uri)
        if not playlist:
            return

        return [Ref.track(uri=t.uri, name=t.name) for t in playlist.tracks]

    def _retrieve_api_tracks(self, playlist):
        return playlist.tracks_paginated()

    def save(self, playlist):
        old_playlist = self._get_or_refresh_playlist(playlist.uri)
        session = self.backend.session
        playlist_id = playlist.uri.split(":")[-1]
        assert old_playlist, f"No such playlist: {playlist.uri}"
        assert session, "No active session"
        upstream_playlist = session.playlist(playlist_id)

        # Playlist rename case
        if old_playlist.name != playlist.name:
            upstream_playlist.edit(title=playlist.name)

        additions = []
        removals = []
        remove_offset = 0
        diff_lines = difflib.ndiff(
            [t.uri for t in old_playlist.tracks], [t.uri for t in playlist.tracks]
        )

        for diff_line in diff_lines:
            if diff_line.startswith("+ "):
                additions.append(diff_line[2:].split(":")[-1])
            else:
                if diff_line.startswith("- "):
                    removals.append(remove_offset)
                remove_offset += 1

        # Process removals in descending order so we don't have to recalculate
        # the offsets while we remove tracks
        if removals:
            logger.info(
                'Removing %d tracks from the playlist "%s"',
                len(removals),
                playlist.name,
            )

            removals.reverse()
            for idx in removals:
                upstream_playlist.remove_by_index(idx)

        # tidalapi currently only supports appending tracks to the end of the
        # playlist
        if additions:
            logger.info(
                'Adding %d tracks to the playlist "%s"', len(additions), playlist.name
            )

            upstream_playlist.add(additions)

        # remove all defunct tracks from cache
        self._calculate_added_and_removed_playlist_ids()
        # force update the whole playlist so all state is good
        self.refresh(playlist.uri)
