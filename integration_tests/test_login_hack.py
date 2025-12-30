from pathlib import Path

import pytest

from .util import mopidy


@pytest.mark.parametrize(
    "type",
    (
        "artist",
        "album",
        "Artist",
        "Album",
        "AlbumArtist",
        "Title",
        "Genre",
        "Date",
        "Composer",
        "Performer",
        "Comment",
    ),
)
def test_link_on_mpc_list_with_hack_login(
    type, spawn, config_dir: Path, tmp_path: Path
):
    with spawn(mopidy(config_dir, "lazy_hack.conf", tmp_path)) as child:
        child.expect("Quality: LOSSLESS")
        child.expect("Authentication: OAuth")
        child.expect("MPD server running")

        with spawn(f"mpc list {type}") as mpc:
            mpc.expect(".* visit https://link.tidal.com/.*")


def test_user_warned_if_lazy_set_implicitly(spawn, config_dir: Path, tmp_path: Path):
    with spawn(mopidy(config_dir, "hack.conf", tmp_path)) as child:
        child.expect("Quality: LOSSLESS")
        child.expect("Authentication: OAuth")
        child.expect("implies lazy connection")
        child.expect("MPD server running")
        child.expect("Starting GLib mainloop")

        with spawn("mpc list artist") as mpc:
            mpc.expect(".* visit https://link.tidal.com/.*")
