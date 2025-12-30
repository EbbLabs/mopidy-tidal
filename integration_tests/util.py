from pathlib import Path


def mopidy(config_dir: Path, config: str, cache_dir: Path) -> str:
    return (
        f"mopidy --config {(config_dir / config).resolve()} "
        f"-o core/cache_dir={cache_dir} "
        f"-o core/data_dir={cache_dir}"
    )
