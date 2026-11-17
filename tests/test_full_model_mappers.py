from datetime import datetime

import mopidy.models as mopidy_models

from mopidy_tidal.full_models_mappers import create_mopidy_album, create_mopidy_artist


class TestCreateMopidyArtist:
    def test_returns_none_if_tidal_artist_none(self):
        assert create_mopidy_artist(tidal_artist=None) is None

    def test_returns_artist_with_uri_and_name(self, make_tidal_artist):
        artist = make_tidal_artist(name="Arty", id=17)

        mopidy_artist = create_mopidy_artist(artist)

        assert mopidy_artist == mopidy_models.Artist(uri="tidal:artist:17", name="Arty")


class TestCreateMopidyAlbum:
    def test_returns_album_with_uri_name_and_artist(
        self, make_tidal_album, make_tidal_artist
    ):
        album = make_tidal_album(name="Alby", id=156)
        mopidy_artist = create_mopidy_artist(make_tidal_artist(name="Arty", id=12))

        mopidy_album = create_mopidy_album(album, [mopidy_artist])

        assert mopidy_album
        assert mopidy_album.uri == "tidal:album:156"
        assert mopidy_album.artists == {
            mopidy_models.Artist(name="Arty", uri="tidal:artist:12")
        }
        assert mopidy_album.name == "Alby"

    def test_date_prefers_release_date(self, make_tidal_album):
        album = make_tidal_album(
            name="Albion",
            id=156,
            release_date=datetime(1995, 6, 7),
            tidal_release_date=datetime(1997, 4, 5),
        )

        mopidy_album = create_mopidy_album(album, None)

        assert mopidy_album
        assert mopidy_album.date == "1995"

    def test_date_falls_back_on_tidal_release_date(self, make_tidal_album):
        album = make_tidal_album(
            name="Albion",
            id=156,
            release_date=None,
            tidal_release_date=datetime(1997, 4, 5),
        )

        mopidy_album = create_mopidy_album(album, None)

        assert mopidy_album
        assert mopidy_album.date == "1997"

    def test_null_when_unknown(self, make_tidal_album):
        album = make_tidal_album(
            name="Albion",
            id=156,
            release_date=None,
            tidal_release_date=None,
        )

        mopidy_album = create_mopidy_album(album, None)

        assert mopidy_album
        assert mopidy_album.date is None

    def test_uses_artist_album_if_no_artist_provided(
        self, make_tidal_album, make_tidal_artist
    ):
        album = make_tidal_album(
            name="Alby", id=156, artist=make_tidal_artist(name="Arty", id=12)
        )

        mopidy_album = create_mopidy_album(album, None)

        assert mopidy_album
        assert mopidy_album.artists == {
            mopidy_models.Artist(name="Arty", uri="tidal:artist:12")
        }


def test_mix_playlist_skips_unavailable_tracks(tidal_tracks):
    from unittest.mock import Mock

    from mopidy_tidal.full_models_mappers import create_mopidy_mix_playlist

    tidal_tracks[0].available = False
    mix = Mock(id="m1", title="Mix", sub_title="Sub")
    mix.items.return_value = tidal_tracks

    playlist = create_mopidy_mix_playlist(mix)

    assert [t.uri for t in playlist.tracks] == [t.uri for t in tidal_tracks[1:]]
