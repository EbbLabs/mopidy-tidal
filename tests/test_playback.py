import pytest

from mopidy_tidal.playback import TidalPlaybackProvider


@pytest.mark.xfail(reason="Requires mock tidal object")
def test_playback(mocker):
    uniq = object()
    session = mocker.Mock(spec=["track"])
    session.mock_add_spec(["track"])
    track = mocker.Mock()
    track.get_url.return_value = uniq
    session.track.return_value = track
    backend = mocker.Mock(session=session)
    audio = mocker.Mock()
    tpp = TidalPlaybackProvider(audio, backend)
    assert tpp.translate_uri("tidal:track:1:2:3") is uniq
    session.track.assert_called_once_with(3)
    track.get_url.assert_called_once()


@pytest.fixture
def playback(mocker):
    session = mocker.Mock(spec=["track", "config"])
    session.config.quality = "high_lossless"
    backend = mocker.Mock(session=session)
    backend.playback_cache_for.return_value = None
    return TidalPlaybackProvider(mocker.Mock(), backend), session


def test_translate_uri_of_unavailable_track_is_none(playback, mocker):
    tpp, session = playback
    session.track.return_value = mocker.Mock(available=False)
    as_stream = mocker.patch("mopidy_tidal.playback.as_stream")

    assert tpp.translate_uri("tidal:track:1:2:3") is None
    as_stream.assert_not_called()


def test_translate_uri_of_available_track_streams_it(playback, mocker):
    tpp, session = playback
    track = mocker.Mock(available=True)
    session.track.return_value = track
    as_stream = mocker.patch("mopidy_tidal.playback.as_stream", return_value="url")

    assert tpp.translate_uri("tidal:track:1:2:3") == "url"
    as_stream.assert_called_once_with(track)
    session.track.assert_called_once_with("3")


def test_translate_uri_looks_the_track_up_once_with_the_playback_cache(
    playback, mocker
):
    tpp, session = playback
    track = mocker.Mock(available=True)
    track.get_url.return_value = "https://example.com/audio/file.flac"
    session.track.return_value = track
    proxy = mocker.Mock()
    proxy.cache.lookup_entry.return_value = None
    proxy.config.local_url.return_value = "http://local/file"
    tpp.backend.playback_cache_for.return_value = proxy
    mocker.patch("mopidy_tidal.playback.cache")

    assert tpp.translate_uri("tidal:track:1:2:3") == "http://local/file"
    session.track.assert_called_once_with("3")


def test_translate_uri_of_cached_track_does_not_need_tidal(playback, mocker):
    tpp, session = playback
    proxy = mocker.Mock()
    proxy.config.local_url.return_value = "http://local/file"
    tpp.backend.playback_cache_for.return_value = proxy
    mocker.patch("mopidy_tidal.playback.cache")

    assert tpp.translate_uri("tidal:track:1:2:3") == "http://local/file"
    session.track.assert_not_called()
