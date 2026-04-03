from __future__ import annotations

import re
import unicodedata
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd


SERIES_RENAME = {
    "value": "series_value",
    "valor": "series_value",
    "fecha": "date",
    "region": "scope",
    "municipio": "scope",
}
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
QUARTER_RE = re.compile(r"^(\d{4})T([1-4])$")


def _normalize_text(value: str) -> str:
    txt = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"\W+", "_", txt.strip().lower())
    return re.sub(r"_+", "_", cleaned).strip("_")


def _read_csv_resilient(path: Path) -> pd.DataFrame:
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    last_error: Exception | None = None
    separators = [";", "\t", ",", None]
    for enc in encodings:
        for sep in separators:
            try:
                return pd.read_csv(path, encoding=enc, sep=sep, engine="python", on_bad_lines="skip")
            except UnicodeDecodeError as exc:
                last_error = exc
            except Exception as exc:  # noqa: BLE001
                last_error = exc
    raise ValueError(f"No se pudo leer CSV histórico {path} con codificaciones soportadas. Último error: {last_error}")


def _parse_ine_periodic_csv(
    df: pd.DataFrame,
    source: str,
    indicator: str,
    scope: str,
    selector: dict[str, str] | None = None,
    scope_column: str | None = None,
) -> pd.DataFrame:
    norm_cols = {_normalize_text(c): c for c in df.columns}
    period_col = next((norm_cols[k] for k in norm_cols if "periodo" in k or k == "period"), None)

    if not period_col:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    filtered = df.copy()
    if selector:
        for raw_key, expected in selector.items():
            normalized_key = _normalize_text(raw_key)
            original_col = norm_cols.get(normalized_key)
            if original_col is None:
                continue
            if str(expected).strip() == "":
                filtered = filtered[
                    filtered[original_col].isna()
                    | filtered[original_col].astype(str).str.strip().eq("")
                ]
                continue
            expected_norm = _normalize_text(str(expected))
            filtered = filtered[
                filtered[original_col]
                .astype(str)
                .apply(_normalize_text)
                .eq(expected_norm)
            ]

    if filtered.empty:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    value_col = None
    candidate_cols = [c for c in filtered.columns if c != period_col]
    best_numeric = -1
    for col in candidate_cols:
        converted = (
            filtered[col]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        numeric_count = pd.to_numeric(converted, errors="coerce").notna().sum()
        if numeric_count > best_numeric:
            best_numeric = int(numeric_count)
            value_col = col

    if not value_col:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    out = filtered[[period_col, value_col]].copy()
    out.columns = ["period_raw", "series_value_raw"]

    resolved_scope_col = None
    if scope_column:
        resolved_scope_col = norm_cols.get(_normalize_text(scope_column))
    if resolved_scope_col is None:
        for candidate in (
            "comunidades_y_ciudades_autonomas",
            "comunidades_autonomas",
            "ccaa",
            "ambito_territorial",
            "total_nacional",
        ):
            resolved_scope_col = norm_cols.get(candidate)
            if resolved_scope_col:
                break
    if resolved_scope_col and resolved_scope_col in filtered.columns:
        out["scope"] = filtered[resolved_scope_col].fillna(scope).astype(str).str.strip()
        out["scope"] = out["scope"].replace("", scope)
    else:
        out["scope"] = scope

    out["date"] = out["period_raw"].astype(str).apply(_parse_quarter_to_date)
    out["series_value"] = (
        out["series_value_raw"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    out["series_value"] = pd.to_numeric(out["series_value"], errors="coerce")
    out = out.dropna(subset=["date", "series_value"])

    if out.empty:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    out["source"] = source
    out["indicator"] = indicator
    return out[["source", "indicator", "scope", "date", "series_value"]]


def _excel_col_to_idx(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    col = 0
    for ch in letters:
        col = col * 26 + (ord(ch.upper()) - ord("A") + 1)
    return col - 1


def _xlsx_sheet_to_rows(path: Path) -> list[list[str | float | None]]:
    with zipfile.ZipFile(path) as zf:
        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        first_sheet = workbook.find("m:sheets/m:sheet", NS)
        if first_sheet is None:
            return []

        rel_id = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        target = None
        for rel in rels:
            if rel.attrib.get("Id") == rel_id:
                target = rel.attrib.get("Target")
                break
        if not target:
            return []

        sheet_root = ET.fromstring(zf.read(f"xl/{target}"))

    rows = []
    for row in sheet_root.findall("m:sheetData/m:row", NS):
        parsed_row = {}
        for cell in row.findall("m:c", NS):
            ref = cell.attrib.get("r", "A1")
            col_idx = _excel_col_to_idx(ref)
            cell_type = cell.attrib.get("t")

            value: str | float | None = None
            if cell_type == "inlineStr":
                text_node = cell.find("m:is/m:t", NS)
                value = text_node.text if text_node is not None else ""
            else:
                val_node = cell.find("m:v", NS)
                raw = val_node.text if val_node is not None else None
                if raw is not None:
                    try:
                        value = float(raw)
                    except ValueError:
                        value = raw

            parsed_row[col_idx] = value

        if not parsed_row:
            continue
        max_col = max(parsed_row.keys())
        full = [None] * (max_col + 1)
        for idx, val in parsed_row.items():
            full[idx] = val
        rows.append(full)

    return rows


def _parse_quarter_to_date(value: str) -> pd.Timestamp | None:
    m = QUARTER_RE.match(value.strip())
    if not m:
        return None
    year = int(m.group(1))
    quarter = int(m.group(2))
    month = quarter * 3
    return pd.Timestamp(year=year, month=month, day=1)


def _load_ine_xlsx(path: Path, source: str, indicator_prefix: str, default_scope: str) -> pd.DataFrame:
    rows = _xlsx_sheet_to_rows(path)
    if not rows:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    header_idx = None
    quarter_cols: list[tuple[int, pd.Timestamp]] = []
    for i, row in enumerate(rows):
        local_q = []
        for j, val in enumerate(row):
            if isinstance(val, str):
                ts = _parse_quarter_to_date(val)
                if ts is not None:
                    local_q.append((j, ts))
        if len(local_q) >= 4:
            header_idx = i
            quarter_cols = local_q
            break

    if header_idx is None:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    records = []
    current_scope = default_scope

    for row in rows[header_idx + 1 :]:
        if not row:
            continue

        label = row[0] if len(row) > 0 else None
        if isinstance(label, str):
            clean_label = label.strip()
            if not clean_label:
                continue

            has_numeric = any(
                (col_idx < len(row) and isinstance(row[col_idx], (int, float)))
                for col_idx, _ in quarter_cols
            )

            if not label.startswith(" ") and not has_numeric:
                current_scope = clean_label
                continue

            if has_numeric:
                metric_slug = re.sub(r"\W+", "_", clean_label.lower()).strip("_")
                indicator = f"{indicator_prefix}_{metric_slug}" if metric_slug else indicator_prefix
                for col_idx, date in quarter_cols:
                    if col_idx >= len(row):
                        continue
                    val = row[col_idx]
                    if isinstance(val, (int, float)):
                        records.append(
                            {
                                "source": source,
                                "indicator": indicator,
                                "scope": current_scope,
                                "date": date,
                                "series_value": float(val),
                            }
                        )

    if not records:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    return pd.DataFrame(records)


def _load_single_source(item: dict) -> pd.DataFrame:
    path = Path(item["path"])
    if not path.exists():
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    source = item.get("source", path.stem)
    indicator = item.get("indicator", path.stem)
    scope = item.get("scope", "Comunidad de Madrid")

    if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        return _load_ine_xlsx(path, source=source, indicator_prefix=indicator, default_scope=scope)

    df = _read_csv_resilient(path)
    parsed_ine = _parse_ine_periodic_csv(
        df,
        source=source,
        indicator=indicator,
        scope=scope,
        selector=item.get("selector"),
        scope_column=item.get("scope_column"),
    )
    if not parsed_ine.empty:
        return parsed_ine

    df = df.rename(columns={k: v for k, v in SERIES_RENAME.items() if k in df.columns})

    if "date" not in df.columns or "series_value" not in df.columns:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    out = df[["date", "series_value"]].copy()
    out["source"] = source
    out["indicator"] = indicator
    out["scope"] = df["scope"] if "scope" in df.columns else scope
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["series_value"] = pd.to_numeric(out["series_value"], errors="coerce")
    return out.dropna(subset=["date", "series_value"])


def load_historical_series(cfg: dict) -> pd.DataFrame:
    files = cfg.get("historical", {}).get("sources", [])
    if not files:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    frames = [_load_single_source(item) for item in files]
    frames = [f for f in frames if not f.empty]

    if not frames:
        return pd.DataFrame(columns=["source", "indicator", "scope", "date", "series_value"])

    return pd.concat(frames, ignore_index=True).sort_values(["indicator", "scope", "date"])


def build_historical_features(historical_df: pd.DataFrame) -> dict:
    if historical_df.empty:
        return {
            "hist_series_count": 0,
            "hist_latest_mean": 0.0,
            "hist_trend_6m_mean": 0.0,
        }

    latest_rows = historical_df.sort_values("date").groupby(["indicator", "scope"], as_index=False).tail(1)
    latest_mean = float(latest_rows["series_value"].mean()) if not latest_rows.empty else 0.0

    trend_values = []
    for _, group in historical_df.groupby(["indicator", "scope"]):
        g = group.sort_values("date")
        if len(g) < 2:
            continue
        tail = g.tail(7)
        first = float(tail["series_value"].iloc[0])
        last = float(tail["series_value"].iloc[-1])
        trend_values.append(last - first)

    trend_mean = float(sum(trend_values) / len(trend_values)) if trend_values else 0.0

    return {
        "hist_series_count": int(historical_df[["indicator", "scope"]].drop_duplicates().shape[0]),
        "hist_latest_mean": latest_mean,
        "hist_trend_6m_mean": trend_mean,
    }
