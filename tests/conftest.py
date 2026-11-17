import json
from concurrent.futures import Future
from typing import Optional
from unittest.mock import Mock

import pytest
from tidalapi import Genre, Session
from tidalapi.album import Album
from tidalapi.artist import Artist
from tidalapi.media import Track
from tidalapi.mix import Mix
from tidalapi.page import Page, PageCategory
from tidalapi.playlist import UserPlaylist
from tidalapi.session import LinkLogin

from mopidy_tidal import context
from mopidy_tidal.backend import TidalBackend
from mopidy_tidal.context import set_config


def _make_mock(mock: Optional[Mock] = None, **kwargs) -> Mock:
    """Make a mock with the desired properties.

    This exists to work around name collisions in `Mock(**kwargs)`, which
    prevents settings some values, such as `name`.  If desired a configured
    mock can be passed in, in which case this is simply a wrapper around
    setting attributes.

    >>> from unittest.mock import Mock
    >>>  # shadowed: sets the *mock name*, not the attribute
    >>> assert Mock(name="foo").name != "foo"
    >>> assert make_mock(name="foo").name == "foo"
    """
    mock = mock or Mock()
    for k, v in kwargs.items():
        setattr(mock, k, v)

    return mock


make_mock = pytest.fixture(lambda: _make_mock)


@pytest.fixture(autouse=True)
def config(tmp_path):
    """Set up config.

    This fixture sets up config in context and removes it after the test.  It
    yields the config dictionary, so if you edit the dictionary you are editing
    the config.
    """

    cfg = {
        "core": {
            "cache_dir": str(tmp_path),
            "data_dir": str(tmp_path),
        },
        "tidal": {
            "client_id": "client_id",
            "client_secret": "client_secret",
            "quality": "LOSSLESS",
            "lazy": False,
            "login_method": "BLOCK",
            "auth_method": "OAUTH2",
            "login_server_port": 8989,
        },
    }
    context.set_config(cfg)
    yield cfg
    context.set_config(None)


@pytest.fixture
def tidal_search(mocker):
    """Provide an uncached tidal_search.

    Tidal search is cached with a decorator, so we have to mock before we
    import anything.  No test should import anything from
    `mopidy_tidal.search`.  Instead use this fixture."""
    # import lru_cache so we can mock the right name in sys.modules without it
    # being overriden.
    from mopidy_tidal import lru_cache  # noqa

    # remove caching, since the cache is created only at import so otherwise we
    # can't remove it
    mocker.patch("lru_cache.SearchCache", lambda x: x)
    from mopidy_tidal.search import tidal_search

    yield tidal_search


def counter(msg: str):
    """A counter for providing sequential names."""
    x = 0
    while True:
        yield msg.format(x)
        x += 1


# Giving each mock a unique name really helps when inspecting funny behaviour.
track_counter = counter("Mock Track #{}")
album_counter = counter("Mock Album #{}")
artist_counter = counter("Mock Artist #{}")
page_counter = counter("Mock Page #{}")
mix_counter = counter("Mock Mix #{}")
genre_counter = counter("Mock Genre #{}")


def _make_tidal_track(
    id: int,
    artist: Artist,
    album: Album,
    name: Optional[str] = None,
    duration: Optional[int] = None,
):
    return _make_mock(
        mock=Mock(spec=Track, name=next(track_counter)),
        id=id,
        name=name or f"Track-{id}",
        full_name=name or f"Track-{id}",
        artist=artist,
        artists=[artist],
        album=album,
        uri=f"tidal:track:{artist.id}:{album.id}:{id}",
        duration=duration or (100 + id),
        track_num=id,
        disc_num=id,
    )


def _make_tidal_artist(*, name: str, id: int, top_tracks: Optional[list[Track]] = None):
    return _make_mock(
        mock=Mock(spec=Artist, name=next(artist_counter)),
        **{
            "id": id,
            "get_top_tracks.return_value": top_tracks,
            "name": name,
        },
    )


def _make_tidal_album(
    *,
    name: str,
    id: int,
    tracks: Optional[list[dict]] = None,
    artist: Optional[Artist] = None,
    **kwargs,
):
    artist = artist or _make_tidal_artist(name="Album Artist", id=id + 1234)
    album = _make_mock(
        mock=Mock(spec=Album, name=next(album_counter)),
        name=name,
        id=id,
        artist=artist,
        artists=[artist],
        **kwargs,
    )
    tracks = [_make_tidal_track(**spec, album=album) for spec in (tracks or [])]
    album.tracks.return_value = tracks
    return album


def _make_tidal_page(*, title: str, categories: list[PageCategory], api_path: str):
    return Mock(spec=Page, title=title, categories=categories, api_path=api_path)


def _make_tidal_mix(*, title: str, sub_title: str, id: int):
    return Mock(
        spec=Mix, title=title, sub_title=sub_title, name=next(mix_counter), id=str(id)
    )


def _make_tidal_genre(*, name: str, path: str):
    return _make_mock(
        mock=Mock(spec=Genre, name=next(genre_counter), path=path), name=name
    )


@pytest.fixture()
def make_tidal_genre():
    return _make_tidal_genre


@pytest.fixture()
def make_tidal_artist():
    return _make_tidal_artist


@pytest.fixture()
def make_tidal_album():
    return _make_tidal_album


@pytest.fixture()
def make_tidal_track():
    return _make_tidal_track


@pytest.fixture()
def make_tidal_page():
    return _make_tidal_page


@pytest.fixture()
def make_tidal_mix():
    return _make_tidal_mix


@pytest.fixture()
def tidal_artists(mocker):
    """A list of tidal artists."""
    artists = [mocker.Mock(spec=Artist, name=f"Artist-{i}") for i in range(2)]
    album = mocker.Mock(spec=Album)
    album.name = "demo album"
    album.id = 7
    for i, artist in enumerate(artists):
        artist.id = i
        artist.name = f"Artist-{i}"
        artist.get_top_tracks.return_value = [
            _make_tidal_track((i + 1) * 100, artist, album)
        ]
    return artists


@pytest.fixture()
def tidal_albums(mocker):
    """A list of tidal albums."""
    albums = [mocker.Mock(spec=Album, name=f"Album-{i}") for i in range(2)]
    artist = mocker.Mock(spec=Artist, name="Album Artist")
    artist.name = "Album Artist"
    artist.id = 1234
    for i, album in enumerate(albums):
        album.id = i
        album.name = f"Album-{i}"
        album.artist = artist
        album.artists = [artist]
        album.tracks.return_value = [_make_tidal_track(i, artist, album)]
    return albums


@pytest.fixture
def tidal_tracks(tidal_artists, tidal_albums):
    """A list of tidal tracks."""
    return [
        _make_tidal_track(i, artist, album)
        for i, (artist, album) in enumerate(zip(tidal_artists, tidal_albums))
    ]


def make_playlist(playlist_id, tracks):
    return _make_mock(
        mock=Mock(spec=UserPlaylist, session=Mock()),
        name=f"Playlist-{playlist_id}",
        id=str(playlist_id),
        uri=f"tidal:playlist:{playlist_id}",
        tracks=tracks,
        tracks_paginated=Mock(return_value=tracks),
        num_tracks=len(tracks),
        last_updated=10,
    )


@pytest.fixture
def tidal_playlists(tidal_tracks):
    return [
        make_playlist(101, tidal_tracks[:2]),
        make_playlist(222, tidal_tracks[1:]),
    ]


def compare_track(tidal, mopidy):
    assert tidal.uri == mopidy.uri
    assert tidal.full_name == mopidy.name
    assert tidal.duration * 1000 == mopidy.length
    assert tidal.disc_num == mopidy.disc_no
    assert tidal.track_num == mopidy.track_no
    compare_artist(tidal.artist, list(mopidy.artists)[0])
    compare_album(tidal.album, mopidy.album)


def compare_artist(tidal, mopidy):
    assert tidal.name == mopidy.name
    assert f"tidal:artist:{tidal.id}" == mopidy.uri


def compare_album(tidal, mopidy):
    assert tidal.name == mopidy.name
    assert f"tidal:album:{tidal.id}" == mopidy.uri


def compare_playlist(tidal, mopidy):
    assert tidal.uri == mopidy.uri
    assert tidal.name == mopidy.name
    _compare(tidal.tracks, mopidy.tracks, "track")


_compare_map = {
    "artist": compare_artist,
    "album": compare_album,
    "track": compare_track,
    "playlist": compare_playlist,
}


def _compare(tidal: list, mopidy: list, type: str):
    """Compare artists, tracks or albums.

    Args:
        tidal: The tidal tracks.
        mopidy: The mopidy tracks.
        type: The type of comparison: one of "artist", "album" or "track".
    """
    assert len(tidal) == len(mopidy)
    for t, m in zip(tidal, mopidy):
        _compare_map[type](t, m)


compare = pytest.fixture(lambda: _compare)


@pytest.fixture
def get_backend(mocker, config):
    # Default to the real test config: its cache directory is a temporary one. A
    # bare MagicMock would make the backend create a `MagicMock/` directory in the
    # current working directory.
    def _get_backend(config=config, audio=mocker.Mock()):
        backend = TidalBackend(config, audio)
        session_factory = mocker.Mock()
        # session = mocker.Mock()
        session = mocker.Mock(spec=SessionForTest)
        session.token_type = "token_type"
        session.session_id = "session_id"
        session.access_token = "access_token"
        session.refresh_token = "refresh_token"
        session.is_pkce = False
        session_factory.return_value = session
        mocker.patch("mopidy_tidal.backend.Session", session_factory)

        # Mock web_auth
        backend.web_auth_server.start_oauth_daemon = mocker.Mock()

        # Mock login_oauth
        url = mocker.Mock(spec=LinkLogin, verification_uri_complete="link.tidal/URI")
        future = mocker.Mock(spec=Future)
        session.login_oauth.return_value = (url, future)

        def save_session_dummy(file_path):
            data = {
                "token_type": {"data": session.token_type},
                "session_id": {"data": session.session_id},
                "access_token": {"data": session.access_token},
                "refresh_token": {"data": session.refresh_token},
                "is_pkce": {"data": session.is_pkce},
                # "expiry_time": {"data": self.expiry_time},
            }
            with file_path.open("w") as outfile:
                json.dump(data, outfile)

        # Saving a session will create a dummy file containing the expected data
        session.save_session_to_file.side_effect = save_session_dummy

        # Always start in logged-out state
        session.check_login.return_value = False

        return backend, config, audio, session_factory, session

    yield _get_backend
    set_config(None)


class SessionForTest(Session):
    """Session has an attribute genre which is set in __init__ doesn't exist on
    the class.  Thus mock gets the spec wrong, i.e. forbids access to genre.
    This is a bug in Session, but until it's fixed we mock it here.

    See https://docs.python.org/3/library/unittest.mock.html#auto-speccing

    Tracked at https://github.com/tamland/python-tidal/issues/192
    """

    genre = None


@pytest.fixture
def session(mocker):
    return mocker.Mock(spec=SessionForTest)


@pytest.fixture
def backend(mocker, session):
    return mocker.Mock(session=session)
