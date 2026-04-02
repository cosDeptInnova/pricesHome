from pathlib import Path
import yaml


def load_config(path: str = "config/config.yaml") -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"No existe el config: {path}")
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
