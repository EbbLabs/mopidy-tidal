from pathlib import Path

import pytest

from .util import mopidy


def test_basic_config_loads_tidal_generates_auth_url(
    spawn, config_dir: Path, tmp_path: Path
):
    with spawn(mopidy(config_dir, "basic.conf", tmp_path)) as child:
        child.expect("Quality: LOSSLESS")
        child.expect("Authentication: OAuth")
        child.expect("Please visit.*https://link.tidal.com/.* to authenticate")


def test_lazy_config_no_connect_to_tidal(spawn, config_dir: Path, tmp_path: Path):
    with spawn(mopidy(config_dir, "lazy.conf", tmp_path)) as child:
        child.expect("Quality: LOSSLESS")
        child.expect("Authentication: OAuth")
        with pytest.raises(AssertionError):
            child.expect("Please visit.*https://link.tidal.com/.* to authenticate")


def test_lazy_config_generates_auth_url_on_access(
    spawn, config_dir: Path, tmp_path: Path
):
    with spawn(mopidy(config_dir, "lazy.conf", tmp_path)) as child:
        child.expect("Quality: LOSSLESS")
        child.expect("Authentication: OAuth")

        with pytest.raises(AssertionError):
            child.expect("Please visit.*https://link.tidal.com/.* to authenticate")

        with spawn("mpc list artist"):
            child.expect("Not logged in.* visit https://link.tidal.com/.*")
