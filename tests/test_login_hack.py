from concurrent.futures import Future
from unittest.mock import PropertyMock

import pytest
from mopidy.models import Album, Artist, Image, Playlist, Ref, SearchResult, Track
from tidalapi.session import LinkLogin

from mopidy_tidal.library import TidalLibraryProvider
from mopidy_tidal.playback import TidalPlaybackProvider
from mopidy_tidal.playlists import TidalPlaylistsProvider


@pytest.fixture
def library_provider(get_backend, config, mocker):
    config["tidal"]["login_method"] = "HACK"
    config["tidal"]["lazy"] = True
    backend, *_, session = get_backend(config=config)
    url = mocker.Mock(spec=LinkLogin, verification_uri_complete="link.tidal/URI")
    future = mocker.Mock(spec=Future)
    future.running.return_value = True
    session.login_oauth.return_value = (url, future)
    backend.on_start()

    # Always start in logged-out state
    session.check_login.return_value = False

    backend._active_session = session

    return backend, TidalLibraryProvider(backend=backend)


class TestLibraryProviderMethods:
    @pytest.mark.parametrize(
        "type, uri",
        [
            ["directory", "tidal:my_albums"],
            ["directory", "tidal:my_artists"],
            ["directory", "tidal:my_playlists"],
            ["directory", "tidal:my_tracks"],
            ["directory", "tidal:moods"],
            ["directory", "tidal:mixes"],
            ["directory", "tidal:genres"],
            ["album", "tidal:album:id"],
            ["artist", "tidal:artist:id"],
            ["playlist", "tidal:playlist:id"],
            ["mood", "tidal:mood:id"],
            ["genre", "tidal:genre:id"],
            ["mix", "tidal:mix:id"],
        ],
    )
    def test_browse_triggers_login(self, type, uri, library_provider):
        backend, lp = library_provider
        assert not backend.logged_in
        assert not backend.logging_in

        browse_results = lp.browse(uri)

        assert not backend._logged_in
        assert backend.logging_in
        assert len(browse_results) == 1
        browse_result = browse_results[0]
        assert isinstance(browse_result, Ref)
        assert browse_result.type == type
        assert "link.tidal/URI" in browse_result.name
        assert browse_result.uri == uri

    def test_get_image_triggers_login(self, library_provider):
        backend, lp = library_provider
        assert not backend.logged_in
        assert not backend.logging_in

        images = lp.get_images(["tidal:playlist:uri"])

        assert not backend.logged_in
        assert backend.logging_in
        assert images == {
            "tidal:playlist:uri": [
                Image(
                    height=150,
                    uri="https://api.qrserver.com/v1/create-qr-code/?size=150x150&data=https%3A%2F%2Flink.tidal%2FURI",
                    width=150,
                )
            ]
        }

    def test_library_lookup_triggers_login(self, library_provider):
        backend, lp = library_provider
        assert not backend.logged_in
        assert not backend.logging_in

        tracks = lp.lookup(["tidal:track:uri"])

        assert not backend.logged_in
        assert backend.logging_in
        assert tracks == [
            Track(
                name="Please visit https://link.tidal/URI to log in.",
                uri="tidal:track:login",
            )
        ]
        assert True

    def test_search_triggers_login(self, library_provider):
        backend, lp = library_provider
        assert not backend.logged_in
        assert not backend.logging_in

        result = lp.search()

        assert not backend.logged_in
        assert backend.logging_in
        assert result == SearchResult(
            albums=[
                Album(
                    name="Please visit https://link.tidal/URI to log in.",
                    uri="tidal:album:login",
                )
            ],
            artists=[
                Artist(
                    name="Please visit https://link.tidal/URI to log in.",
                    uri="tidal:artist:login",
                )
            ],
            tracks=[
                Track(
                    name="Please visit https://link.tidal/URI to log in.",
                    uri="tidal:track:login",
                )
            ],
        )

    @pytest.mark.parametrize("field", ("artist", "album", "track"))
    def test_get_distinct_triggers_login(self, field, library_provider):
        backend, lp = library_provider
        assert not backend.logged_in
        assert not backend.logging_in

        result = lp.get_distinct(field)

        assert not backend.logged_in
        assert backend.logging_in
        assert result == {"Please visit https://link.tidal/URI to log in."}


@pytest.fixture
def playlist_provider(get_backend, config, mocker):
    config["tidal"]["login_method"] = "HACK"
    config["tidal"]["lazy"] = True
    backend, *_, session = get_backend(config=config)
    session.check_login.return_value = False
    url = mocker.Mock(spec=LinkLogin, verification_uri_complete="link.tidal/URI")
    future = mocker.Mock(spec=Future)
    future.running.return_value = True
    session.login_oauth.return_value = (url, future)
    backend.on_start()
    return backend, TidalPlaylistsProvider(backend=backend)


class TestPlaylistMethods:
    def test_playlist_lookup_triggers_login(self, playlist_provider):
        backend, pp = playlist_provider
        assert not backend.logged_in
        assert not backend.logging_in

        result = pp.lookup("tidal:playlist:uri")

        assert not backend.logged_in
        assert backend.logging_in
        assert result == Playlist(
            name="Please visit https://link.tidal/URI to log in.",
            tracks=[
                Track(
                    name="Please visit https://link.tidal/URI to log in.",
                    uri="tidal:track:login",
                )
            ],
            uri="tidal:playlist:login",
        )

    def test_playlist_refresh_triggers_login(self, playlist_provider):
        backend, pp = playlist_provider
        assert not backend.logged_in
        assert not backend.logging_in

        result = pp.refresh("tidal:playlist:uri")

        assert not backend.logged_in
        assert backend.logging_in
        assert result == {
            "tidal:playlist:uri": Playlist(
                name="Please visit https://link.tidal/URI to log in.",
                tracks=[
                    Track(
                        name="Please visit https://link.tidal/URI to log in.",
                        uri="tidal:track:login",
                    )
                ],
                uri="tidal:playlist:login",
            )
        }

    def test_playlist_as_list_triggers_login(self, playlist_provider):
        backend, pp = playlist_provider
        assert not backend.logged_in
        assert not backend.logging_in

        result = pp.as_list()

        assert not backend.logged_in
        assert backend.logging_in
        assert result == [
            Ref(
                name="Please visit https://link.tidal/URI to log in.",
                type="playlist",
                uri="tidal:playlist:login",
            )
        ]

    def test_continues_unfazed_when_already_logged_in(
        self, playlist_provider, mocker, tidal_playlists
    ):
        backend, pp = playlist_provider
        backend._logged_in = True
        audiof = backend.data_dir / "login_audio/URI.ogg"

        session = mocker.Mock()
        session.user.favorites.playlists_paginated.return_value = tidal_playlists

        # Patch the session property on the backend's class, so the instance returns your mock session
        mocker.patch.object(
            type(backend), "session", new_callable=PropertyMock, return_value=session
        )

        mocker.patch("tidalapi.workers.get_items", lambda x: x)

        assert backend.logged_in
        assert not backend.logging_in

        assert pp.as_list() == [
            Ref(name="Playlist-101", type="playlist", uri="tidal:playlist:101"),
            Ref(name="Playlist-222", type="playlist", uri="tidal:playlist:222"),
        ]

        assert not audiof.exists()


@pytest.fixture
def playback_provider(get_backend, config, mocker):
    config["tidal"]["login_method"] = "HACK"
    config["tidal"]["lazy"] = True
    backend, *_, session = get_backend(config=config)
    session.check_login.return_value = False
    url = mocker.Mock(spec=LinkLogin, verification_uri_complete="link.tidal/URI")
    future = mocker.Mock(spec=Future)
    future.running.return_value = True
    session.login_oauth.return_value = (url, future)
    backend.on_start()

    return backend, TidalPlaybackProvider(audio=mocker.Mock(), backend=backend)


class TestPlaybackMethods:
    def test_audio_downloaded(self, playback_provider, mocker):
        backend, pp = playback_provider
        get = mocker.Mock(**{"return_value.content": b"mock audio"})
        mocker.patch("login_hack.get", get)
        audiof = backend.data_dir / "login_audio/URI.ogg"
        assert not audiof.exists()
        assert not backend.logged_in
        assert not backend.logging_in

        result = pp.translate_uri("tidal:track:1:2:3")

        assert not backend.logged_in
        assert backend.logging_in
        assert result == audiof.as_uri()
        get.assert_called_once()
        assert audiof.read_bytes() == b"mock audio"

    def test_failed_audio_download_returns_None(self, playback_provider, mocker):
        backend, pp = playback_provider
        get = mocker.Mock(**{"return_value.raise_for_status": Exception()})
        mocker.patch("login_hack.get", get)
        audiof = backend.data_dir / "login_audio/URI.ogg"
        assert not audiof.exists()
        assert not backend.logged_in
        assert not backend.logging_in

        result = pp.translate_uri("tidal:track:1:2:3")

        assert not backend.logged_in
        assert backend.logging_in
        assert result is None
        get.assert_called_once()
        assert not audiof.exists()

    def test_downloaded_audio_removed_on_next_access(
        self, playback_provider, mocker, tidal_playlists
    ):
        backend, pp = playback_provider
        get = mocker.Mock(**{"return_value.content": b"mock audio"})
        mocker.patch("login_hack.get", get)
        audiof = backend.data_dir / "login_audio/URI.ogg"

        result = pp.translate_uri("tidal:track:1:2:3")

        assert not backend.logged_in
        assert backend.logging_in
        assert result == audiof.as_uri()
        get.assert_called_once()
        assert audiof.read_bytes() == b"mock audio"
        backend._login_future.running.return_value = False
        assert not backend.logging_in

        # Setup mock session with playlists_paginated only
        session = mocker.Mock()
        session.user.favorites.playlists_paginated.return_value = tidal_playlists

        # Patch the read-only session property on backend
        mocker.patch.object(
            type(backend), "session", new_callable=PropertyMock, return_value=session
        )

        # Patch get_items as before
        mocker.patch("tidalapi.workers.get_items", lambda x: x)

        backend._logged_in = True
        tpp = TidalPlaylistsProvider(backend=backend)
        assert tpp.as_list() == [
            Ref(name="Playlist-101", type="playlist", uri="tidal:playlist:101"),
            Ref(name="Playlist-222", type="playlist", uri="tidal:playlist:222"),
        ]

        # The audio file should have been removed on access after login
        assert not audiof.exists()


class TestConfig:
    def test_login_hack_implies_lazy_connect_even_if_set_to_false(
        self, config, get_backend
    ):
        config["tidal"]["login_method"] = "HACK"
        config["tidal"]["lazy"] = False
        backend, *_ = get_backend(config=config)
        assert not backend.lazy_connect

        backend.on_start()

        assert backend.lazy_connect
