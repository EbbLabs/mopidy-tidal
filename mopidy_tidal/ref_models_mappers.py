from __future__ import unicode_literals

import logging

from mopidy.models import Ref
from tidalapi import Album, Artist, Mix, Playlist, Track
from tidalapi.mix import MixType

from mopidy_tidal.helpers import is_available

logger = logging.getLogger(__name__)


def create_root():
    return [
        Ref.directory(uri="tidal:home", name="Home"),
        Ref.directory(uri="tidal:for_you", name="For You"),
        Ref.directory(uri="tidal:explore", name="Explore"),
        Ref.directory(uri="tidal:hires", name="HiRes"),
        Ref.directory(uri="tidal:genres", name="Genres"),
        Ref.directory(uri="tidal:moods", name="Moods"),
        Ref.directory(uri="tidal:mixes", name="My Mixes"),
        Ref.directory(uri="tidal:my_artists", name="My Artists"),
        Ref.directory(uri="tidal:my_albums", name="My Albums"),
        Ref.directory(uri="tidal:my_playlists", name="My Playlists"),
        Ref.directory(uri="tidal:my_tracks", name="My Tracks"),
        Ref.directory(uri="tidal:my_mixes", name="Mixes & Radio"),
    ]


def create_artists(tidal_artists):
    return [create_artist(a) for a in tidal_artists]


def create_artist(tidal_artist):
    return Ref.artist(
        uri="tidal:artist:" + str(tidal_artist.id), name=tidal_artist.name
    )


def create_playlists(tidal_playlists):
    return [create_playlist(p) for p in tidal_playlists]


def create_playlist(tidal_playlist):
    return Ref.playlist(
        uri="tidal:playlist:" + str(tidal_playlist.id), name=tidal_playlist.name
    )


def create_moods(tidal_moods):
    return [create_mood(m) for m in tidal_moods]


def create_mood(tidal_mood):
    mood_id = tidal_mood.api_path.split("/")[-1]
    return Ref.directory(uri="tidal:mood:" + mood_id, name=tidal_mood.title)


def create_genres(tidal_genres):
    return [create_genre(m) for m in tidal_genres]


def create_genre(tidal_genre):
    genre_id = tidal_genre.path
    return Ref.directory(uri="tidal:genre:" + genre_id, name=tidal_genre.name)


def create_category_directories(uri, tidal_page):
    res = []
    for idx, category in enumerate(tidal_page.categories):
        if category.title == "":
            res.extend(
                [create_mixed_entry(item) for idx, item in enumerate(category.items)]
            )
        else:
            res.append(create_category_directory(uri, idx, category.title))

    # Remove None/Unsupported entries
    res_filtered = [i for i in res if i is not None]
    return res_filtered


def create_category_directory(uri, idx, name):
    return Ref.directory(uri="{}:category:{}".format(uri, idx), name=name)


def create_mixed_directory(tidal_mixed):
    res = [create_mixed_entry(m) for m in tidal_mixed]
    # Remove None/Unsupported entries
    res_filtered = [i for i in res if i is not None]
    return res_filtered


def create_mixed_entry(tidal_mixed):
    if isinstance(tidal_mixed, Mix):
        return Ref.playlist(
            uri="tidal:mix:" + tidal_mixed.id,
            name=f"{tidal_mixed.title} ({tidal_mixed.sub_title})",
        )
    elif isinstance(tidal_mixed, Album):
        return Ref.album(
            uri="tidal:album:" + str(tidal_mixed.id),
            name=f"{tidal_mixed.name} ({tidal_mixed.artist.name})",
        )
    elif isinstance(tidal_mixed, Playlist):
        return Ref.playlist(
            uri="tidal:playlist:" + str(tidal_mixed.id),
            name=f"{tidal_mixed.name}",
        )
    elif isinstance(tidal_mixed, Track):
        return create_track(tidal_mixed)
    elif isinstance(tidal_mixed, Artist):
        return create_artist(tidal_mixed)
    else:
        if hasattr(tidal_mixed, "api_path"):
            # Objects containing api_path are usually pages and must be processed further
            return Ref.directory(
                uri="tidal:page:" + tidal_mixed.api_path, name=tidal_mixed.title
            )
        elif hasattr(tidal_mixed, "artifact_id"):
            # Objects containing artifact_id can be viewed directly
            explore_id = tidal_mixed.artifact_id
            name = f"{tidal_mixed.short_header} ({tidal_mixed.short_sub_header})"
            if tidal_mixed.type == "PLAYLIST":
                return Ref.playlist(
                    uri="tidal:playlist:" + explore_id,
                    name=name,
                )
            else:
                # Unsupported type (eg. interview, exturl)
                return None
        else:
            # Unsupported type (eg. Video)
            return None


def create_mixes(tidal_mixes):
    res = [create_mix(m) for m in tidal_mixes]
    # Remove None/Unsupported entries
    res_filtered = [i for i in res if i is not None]
    return res_filtered


def create_mix(tidal_mix):
    if tidal_mix.mix_type is MixType.video_daily:
        # Skip video mixes (not supported)
        return None
    else:
        return Ref.playlist(
            uri="tidal:mix:" + tidal_mix.id,
            name=f"{tidal_mix.title} ({tidal_mix.sub_title})",
        )


def create_albums(tidal_albums):
    return [create_album(a) for a in tidal_albums]


def create_album(tidal_album):
    return Ref.album(uri="tidal:album:" + str(tidal_album.id), name=tidal_album.name)


def create_tracks(tidal_tracks):
    # Unavailable tracks are listed by TIDAL but can't be played back
    return [create_track(t) for t in tidal_tracks if is_available(t)]


def create_track(tidal_track):
    uri = "tidal:track:{0}:{1}:{2}".format(
        tidal_track.artist.id, tidal_track.album.id, tidal_track.id
    )
    return Ref.track(uri=uri, name=tidal_track.name)
