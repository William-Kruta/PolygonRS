import os
import json
import platform
from pathlib import Path

ENV_VAR = "POLYGON_RS_DB"
CONFIG_DIR = "polygonrs"
CONFIG_FILE = "config.json"
DEFAULT_DB = "polygon_rs.db"


def _get_data_dir() -> Path:
    system = platform.system()
    if system == "Windows":
        # %LOCALAPPDATA%/polygonrs  (local, not roaming)
        base = os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")
    elif system == "Darwin":
        # ~/Library/Application Support/polygonrs
        base = Path.home() / "Library" / "Application Support"
    else:
        # ~/.local/share/polygonrs  (XDG data, not config)
        base = os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")

    return Path(base) / CONFIG_DIR


def get_db_path() -> Path:
    # 1. Environment variable
    env_path = os.environ.get(ENV_VAR, "")
    if env_path:
        return Path(env_path)

    # 2. Config file
    data_dir = _get_data_dir()
    config_path = data_dir / CONFIG_FILE

    if config_path.exists():
        try:
            with open(config_path) as f:
                config = json.load(f)
            db_path = config.get("database", "")
            if db_path:
                return Path(db_path)
        except (json.JSONDecodeError, OSError):
            pass

    # 3. Default: inside the data directory
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / DEFAULT_DB
