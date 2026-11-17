import logging

import pytest
from tidalapi.media import ManifestMimeType

from mopidy_tidal.playback import TidalPlaybackProvider, as_stream, mpd_stream_info

# The attributes of interest from an MPD manifest for a lossless stream
FLAC_MPD = (
    '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static">'
    '<Period id="0"><AdaptationSet id="0" contentType="audio" group="main">'
    '<Representation id="FLAC,44100,24" codecs="flac" bandwidth="2938420" '
    'audioSamplingRate="44100"/>'
    "</AdaptationSet></Period></MPD>"
)


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


def test_mpd_stream_info_reads_codec_and_bitrate():
    assert mpd_stream_info(FLAC_MPD) == ("flac", "2938kbps")


@pytest.mark.parametrize("mpd", ["", "<MPD/>", "not xml at all", '<A codecs="">'])
def test_mpd_stream_info_never_fails(mpd):
    assert mpd_stream_info(mpd) == ("unknown", "unknown")


def _mpd_track(mocker, data):
    stream = mocker.Mock(
        manifest_mime_type=ManifestMimeType.MPD,
        audio_quality="HI_RES_LOSSLESS",
        bit_depth=24,
        sample_rate=44100,
    )
    stream.get_manifest_data.return_value = data
    return mocker.Mock(id=77640618, get_stream=mocker.Mock(return_value=stream))


def test_as_stream_logs_codec_and_bitrate_of_mpd_streams(mocker, caplog):
    track = _mpd_track(mocker, FLAC_MPD)

    with caplog.at_level(logging.INFO, logger="mopidy_tidal.playback"):
        uri = as_stream(track)

    assert uri.startswith("file://") and uri.endswith("manifest.mpd")
    assert (
        "Starting playback of track:77640618, "
        "(quality:HI_RES_LOSSLESS, codec:flac, 24bit/44100Hz, bitrate:2938kbps)"
    ) in caplog.text


def test_as_stream_of_mpd_without_stream_info_still_plays(mocker, caplog):
    track = _mpd_track(mocker, "<MPD/>")

    with caplog.at_level(logging.INFO, logger="mopidy_tidal.playback"):
        uri = as_stream(track)

    assert uri.startswith("file://")
    assert "codec:unknown, 24bit/44100Hz, bitrate:unknown" in caplog.text


def test_as_stream_without_mpd_manifest_fails(mocker):
    with pytest.raises(AttributeError):
        as_stream(_mpd_track(mocker, ""))
