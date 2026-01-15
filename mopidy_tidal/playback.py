from __future__ import unicode_literals

from typing import TYPE_CHECKING

from login_hack import speak_login_hack

if TYPE_CHECKING:  # pragma: no cover
    from mopidy_tidal.backend import TidalBackend

import logging
from pathlib import Path

from mopidy import backend
from tidalapi import Quality
from tidalapi.media import ManifestMimeType

from . import Extension, context

logger = logging.getLogger(__name__)


class TidalPlaybackProvider(backend.PlaybackProvider):
    backend: "TidalBackend"

    @speak_login_hack
    def translate_uri(self, uri):
        logger.info("TIDAL uri: %s", uri)
        parts = uri.split(":")
        track_id = int(parts[4])
        session = self.backend.session
        if session.config.quality == Quality.hi_res_lossless:
            if "HIRES_LOSSLESS" in session.track(track_id).media_metadata_tags:
                logger.info("Playback quality: %s", session.config.quality)
            else:
                logger.info(
                    "No HIRES_LOSSLESS available for this track; Using playback quality: %s",
                    "LOSSLESS",
                )

        stream = session.track(track_id).get_stream()
        logger.info("MimeType:{}".format(stream.manifest_mime_type))

        if stream.manifest_mime_type == ManifestMimeType.MPD:
            logger.info(
                "Starting playback of track:{}, (quality:{}, {}bit/{}Hz)".format(
                    track_id,
                    stream.audio_quality,
                    stream.bit_depth,
                    stream.sample_rate,
                )
            )

            data = stream.get_manifest_data()
            if data:
                mpd_path = Path(
                    Extension.get_cache_dir(context.get_config()), "manifest.mpd"
                )
                with open(mpd_path, "w") as file:
                    file.write(data)

                return "file://{}".format(mpd_path)
            else:
                raise AttributeError("No MPD manifest available!")
        elif stream.manifest_mime_type == ManifestMimeType.BTS:
            manifest = stream.get_stream_manifest()
            logger.info(
                "Starting playback of track:{}, (quality:{}, codec:{}, {}bit/{}Hz)".format(
                    track_id,
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
