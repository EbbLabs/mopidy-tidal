from __future__ import unicode_literals

import logging
import os
import sys

from mopidy import config, ext

__version__ = "0.3.13"

# TODO: If you need to log, use loggers named after the current Python module
logger = logging.getLogger(__name__)

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)


class Extension(ext.Extension):
    dist_name = "Mopidy-Tidal"
    ext_name = "tidal"
    version = __version__

    def get_default_config(self):
        conf_file = os.path.join(os.path.dirname(__file__), "ext.conf")
        return config.read(conf_file)

    def get_config_schema(self):
        schema = super().get_config_schema()
        schema["quality"] = config.String(
            choices=["HI_RES_LOSSLESS", "LOSSLESS", "HIGH", "LOW"]
        )
        schema["client_id"] = config.String(optional=True)
        schema["client_secret"] = config.String(optional=True)
        schema["playlist_cache_refresh_secs"] = config.Integer(optional=True)
        schema["lazy"] = config.Boolean(optional=True)
        schema["login_method"] = config.String(choices=["BLOCK", "HACK", "AUTO"])
        schema["auth_method"] = config.String(optional=True, choices=["OAUTH", "PKCE"])
        schema["login_server_port"] = config.Integer(
            optional=True, choices=range(8000, 9000)
        )
        schema["track_cache"] = config.Boolean(optional=True)
        return schema

    def setup(self, registry):
        from .backend import TidalBackend

        registry.add("backend", TidalBackend)
