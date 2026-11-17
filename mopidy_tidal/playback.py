from __future__ import unicode_literals

import re
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

import tidalapi
from login_hack import speak_login_hack
from tidalapi.exceptions import URLNotAvailable

from mopidy_tidal.gstreamer_proxy import cache
from mopidy_tidal.helpers import is_available

if TYPE_CHECKING:  # pragma: no cover
    from mopidy_tidal.backend import TidalBackend

import logging
from pathlib import Path

from mopidy import backend
from tidalapi import Quality
from tidalapi.media import ManifestMimeType

from . import Extension, context

logger = logging.getLogger(__name__)


def mpd_stream_info(mpd: str) -> tuple[str, str]:
    """Codec and bitrate of an MPD manifest, for logging.

    The manifest is deliberately not parsed with tidalapi/mpegdash, which fails on
    manifests it doesn't understand. Reading two attributes from the text can't fail:
    anything that is missing is reported as "unknown".
    """
    codec = re.search(r'\bcodecs="([^"]+)"', mpd)
    bandwidth = re.search(r'\bbandwidth="(\d+)"', mpd)
    return (
        codec.group(1) if codec else "unknown",
        f"{round(int(bandwidth.group(1)) / 1000)}kbps" if bandwidth else "unknown",
    )


def as_stream(track: tidalapi.Track) -> str:
    stream = track.get_stream()
    logger.info("MimeType:{}".format(stream.manifest_mime_type))

    if stream.manifest_mime_type == ManifestMimeType.MPD:
        data = stream.get_manifest_data()
        if data:
            codec, bitrate = mpd_stream_info(data)
            logger.info(
                "Starting playback of track:{}, "
                "(quality:{}, codec:{}, {}bit/{}Hz, bitrate:{})".format(
                    track.id,
                    stream.audio_quality,
                    codec,
                    stream.bit_depth,
                    stream.sample_rate,
                    bitrate,
                )
            )
            mpd_path = Path(
                Extension.get_cache_dir(context.get_config()), "manifest.mpd"
            )
            with open(mpd_path, "w") as file:
                file.write(data)

            return "file://{}".format(mpd_path)
        else:
            raise AttributeError("No MPD manifest available!")
    else:
        assert stream.manifest_mime_type == ManifestMimeType.BTS
        manifest = stream.get_stream_manifest()
        logger.info(
            "Starting playback of track:{}, (quality:{}, codec:{}, {}bit/{}Hz)".format(
                track.id,
                stream.audio_quality,
                manifest.get_codecs(),
                stream.bit_depth,
                stream.sample_rate,
            )
        )
        urls = manifest.get_urls()
        if isinstance(urls, list):
            return urls[0]
        else:
            return urls


class TidalPlaybackProvider(backend.PlaybackProvider):
    backend: "TidalBackend"

    def _track(self, uri: str) -> tidalapi.Track:
        parts = uri.split(":")
        track_id = parts[4]
        session = self.backend.session
        assert session
        track = session.track(track_id)

        if session.config.quality == Quality.hi_res_lossless:
            if "HIRES_LOSSLESS" in track.media_metadata_tags:
                logger.info("Playback quality: %s", session.config.quality)
            else:
                logger.info(
                    "No HIRES_LOSSLESS available for this track; Using playback quality LOSSLESS",
                )
        return track

    @speak_login_hack
    def translate_uri(self, uri) -> Optional[str]:
        logger.info("TIDAL uri: %s", uri)

        proxy = self.backend.playback_cache_for(uri)
        id = cache.TidalID(uri) if proxy else None
        if proxy and (entry := proxy.cache.lookup_entry(id)):
            return proxy.config.local_url(entry.path.decode())

        track = self._track(uri)
        if not is_available(track):
            # Returning None marks the track as unplayable, and Mopidy skips it
            logger.warning("Track %s is not available for playback, skipping", uri)
            return None

        if proxy:
            try:
                url = track.get_url()
            except URLNotAvailable:
                logger.info(
                    "No direct url available for %s, falling back to stream", uri
                )
            else:
                path = cache.Path(urlparse(url).path.encode())
                proxy.cache.insert_path(id, path)
                return proxy.config.local_url(url)

        return as_stream(track)
