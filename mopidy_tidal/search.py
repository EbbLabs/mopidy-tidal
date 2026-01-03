from __future__ import unicode_literals

import logging
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import IntEnum
from typing import (
    Callable,
    Collection,
    Iterable,
    List,
    Mapping,
    Sequence,
    Tuple,
    Type,
    Union,
)

from lru_cache import SearchCache
from tidalapi.album import Album
from tidalapi.artist import Artist
from tidalapi.media import Track

from mopidy_tidal.full_models_mappers import (
    create_mopidy_albums,
    create_mopidy_artists,
    create_mopidy_tracks,
)
from mopidy_tidal.utils import remove_watermark

logger = logging.getLogger(__name__)


class SearchField(IntEnum):
    ANY = 0
    ARTIST = 1
    ALBUMARTIST = 2
    ALBUM = 3
    TITLE = 4


@dataclass
class SearchFieldMeta:
    field: SearchField
    request_field: str
    results_fields: Sequence[str]
    model_classes: Collection[Type[Union[Artist, Album, Track]]]
    mappers: Sequence[Callable[[Collection], Sequence]]


fields_meta = {
    meta.field: meta
    for meta in [
        SearchFieldMeta(
            SearchField.ANY,
            request_field="any",
            results_fields=("artists", "albums", "tracks"),
            model_classes=(Artist, Album, Track),
            mappers=(create_mopidy_artists, create_mopidy_albums, create_mopidy_tracks),
        ),
        SearchFieldMeta(
            SearchField.ARTIST,
            request_field="artist",
            results_fields=("artists",),
            model_classes=(Artist,),
            mappers=(create_mopidy_artists,),
        ),
        SearchFieldMeta(
            SearchField.ALBUMARTIST,
            request_field="albumartist",
            results_fields=("artists",),
            model_classes=(Artist,),
            mappers=(create_mopidy_artists,),
        ),
        SearchFieldMeta(
            SearchField.ALBUM,
            request_field="album",
            results_fields=("albums",),
            model_classes=(Album,),
            mappers=(create_mopidy_albums,),
        ),
        SearchFieldMeta(
            SearchField.TITLE,
            request_field="track_name",
            results_fields=("tracks",),
            model_classes=(Track,),
            mappers=(create_mopidy_tracks,),
        ),
    ]
}


def _get_flattened_query_and_field_meta(
    query: Mapping[str, str],
) -> Tuple[str, SearchFieldMeta]:
    q = " ".join(
        query[field]
        for field in ("any", "artist", "album", "track_name")
        if query.get(field)
    )

    fields_by_request_field = {
        field_meta.request_field: field_meta for field_meta in fields_meta.values()
    }

    matched_field_meta = fields_by_request_field["any"]
    for attr in ("track_name", "album", "artist", "albumartist"):
        field_meta = fields_by_request_field.get(attr)
        if field_meta and query.get(attr):
            matched_field_meta = field_meta
            break

    return q, matched_field_meta


def _get_exact_result(
    query: Mapping,
    results: Tuple[Iterable[Artist], Iterable[Album], Iterable[Track]],
    field_meta: SearchFieldMeta,
) -> Tuple[List[Artist], List[Album], List[Track]]:
    query_value = query[field_meta.request_field]
    filtered_results = [], [], []

    for i, attr in enumerate(
        (SearchField.TITLE, SearchField.ALBUM, SearchField.ARTIST)
    ):
        if attr == field_meta.field:
            item = next(
                (
                    res
                    # TODO: why not results[-i-1]?
                    for res in results[len(results) - i - 1]
                    if res.name and res.name.lower() == query_value.lower()
                ),
                None,
            )

            if item:
                filtered_results[len(results) - i - 1].append(item)
            break

    return filtered_results


def _expand_artist_top_tracks(artist: Artist) -> List[Track]:
    return artist.get_top_tracks(limit=25)


def _expand_album_tracks(album: Album) -> List[Track]:
    return album.tracks()


def _expand_results_tracks(
    results: Tuple[List[Artist], List[Album], List[Track]],
) -> Tuple[List[Artist], List[Album], List[Track]]:
    results_ = list(results)
    artists = results_[0]
    albums = results_[1]

    with ThreadPoolExecutor(4, thread_name_prefix="mopidy-tidal-search-") as pool:
        pool_res = pool.map(_expand_artist_top_tracks, artists)
        for tracks in pool_res:
            results_[2].extend(tracks)

        pool_res = pool.map(_expand_album_tracks, albums)
        for tracks in pool_res:
            results_[2].extend(tracks)

    # Remove any duplicate tracks from results
    tracks_by_id = OrderedDict({track.id: track for track in results_[2]})
    results_[2] = list(tracks_by_id.values())
    return tuple(results_)


@SearchCache
def tidal_search(session, query, exact=False):
    logger.info("Searching Tidal for: %r", query)
    query = query.copy()

    for field, value in query.items():
        if hasattr(value, "__iter__") and not isinstance(value, (str, bytes)):
            value = value[0]
        query[field] = remove_watermark(value)

    query_string, field_meta = _get_flattened_query_and_field_meta(query)

    results = [[], [], []]  # artists, albums, tracks
    api_results = session.search(query_string, models=field_meta.model_classes)

    for i, field_type in enumerate(
        (SearchField.ARTIST, SearchField.ALBUM, SearchField.TITLE)
    ):
        meta = fields_meta[field_type]
        results_field = meta.results_fields[0]
        mapper = meta.mappers[0]

        if not (results_field in api_results and results_field in meta.results_fields):
            continue

        results[i] = api_results[results_field]

    if exact:
        results = list(_get_exact_result(query, tuple(results), field_meta))

    _expand_results_tracks(results)
    for i, field_type in enumerate(
        (SearchField.ARTIST, SearchField.ALBUM, SearchField.TITLE)
    ):
        meta = fields_meta[field_type]
        mapper = meta.mappers[0]
        results[i] = mapper(results[i])

    return tuple(results)
