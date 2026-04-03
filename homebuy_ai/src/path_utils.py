from __future__ import annotations

from difflib import SequenceMatcher
from pathlib import Path
import unicodedata


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _normalize_filename(value: str) -> str:
    txt = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in txt.lower() if ch.isalnum() or ch in {".", "_"})


def resolve_data_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.exists():
        return candidate

    project_relative = PROJECT_ROOT / candidate
    if project_relative.exists():
        return project_relative

    lookup_dir = project_relative.parent if project_relative.parent.exists() else candidate.parent
    if not lookup_dir.exists():
        return candidate

    target_name = _normalize_filename(candidate.name)
    best_match: tuple[float, Path] | None = None

    for file_path in lookup_dir.iterdir():
        if not file_path.is_file():
            continue
        if candidate.suffix and file_path.suffix.lower() != candidate.suffix.lower():
            continue

        score = SequenceMatcher(None, target_name, _normalize_filename(file_path.name)).ratio()
        if score < 0.85:
            continue

        if best_match is None or score > best_match[0]:
            best_match = (score, file_path)

    return best_match[1] if best_match else candidate
