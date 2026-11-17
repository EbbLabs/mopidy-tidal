from unittest.mock import Mock

from mopidy_tidal import ref_models_mappers as rmm


def test_root_contains_entries_for_eachfield():
    root = rmm.create_root()

    uri_map = {
        "tidal:genres": "Genres",
        "tidal:moods": "Moods",
        "tidal:mixes": "My Mixes",
        "tidal:my_artists": "My Artists",
        "tidal:my_albums": "My Albums",
        "tidal:my_playlists": "My Playlists",
        "tidal:my_tracks": "My Tracks",
    }
    for uri, name in uri_map.items():
        ref = next(x for x in root if x.uri == uri)
        assert ref.name == name


def test_create_tracks_skips_unavailable_tracks(tidal_tracks):
    tidal_tracks[1].available = False

    refs = rmm.create_tracks(tidal_tracks)

    assert [r.uri for r in refs] == [
        t.uri for i, t in enumerate(tidal_tracks) if i != 1
    ]


def test_create_tracks_keeps_items_without_availability(tidal_tracks):
    item = Mock(spec=["artist", "album", "id", "name"])
    item.artist = tidal_tracks[0].artist
    item.album = tidal_tracks[0].album
    item.id = 99
    item.name = "No flag"

    assert len(rmm.create_tracks([item])) == 1
