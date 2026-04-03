from __future__ import annotations

import re
from typing import Any
import unicodedata

import pandas as pd
import requests

from src.path_utils import resolve_data_path


REQUIRED_COLUMNS = {
    "listing_id",
    "date",
    "municipio",
    "price",
    "m2",
    "rooms",
    "garage",
    "ascensor",
}


CSV_ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
QUARTER_RE = re.compile(r"^(\d{4})T([1-4])$")


def _normalize_text(value: str) -> str:
    txt = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"\W+", "_", txt.strip().lower())
    return re.sub(r"_+", "_", cleaned).strip("_")


def _read_csv_resilient(path: str) -> pd.DataFrame:
    last_error: Exception | None = None
    separators = [";", "\t", ",", None]
    for enc in CSV_ENCODINGS:
        for sep in separators:
            try:
                return pd.read_csv(path, encoding=enc, sep=sep, engine="python", on_bad_lines="skip")
            except UnicodeDecodeError as exc:
                last_error = exc
            except Exception as exc:  # noqa: BLE001
                last_error = exc
    raise ValueError(f"No se pudo leer CSV en {path} con codificaciones soportadas: {CSV_ENCODINGS}. Error: {last_error}")


def _ensure_tipologia(df: pd.DataFrame) -> pd.DataFrame:
    if "tipologia" in df.columns:
        return df

    out = df.copy()
    out["tipologia"] = out.apply(
        lambda r: _infer_tipologia(float(r.get("m2", 0)), float(r.get("rooms", 0))),
        axis=1,
    )
    return out


def _infer_tipologia(m2: float, rooms: float) -> str:
    if m2 >= 120 or rooms >= 4:
        return "familiar"
    if m2 <= 65 or rooms <= 1:
        return "compacta"
    return "media"


def _normalize_columns(df: pd.DataFrame, mapping: dict[str, str] | None) -> pd.DataFrame:
    if not mapping:
        return df
    reverse = {origin: dest for dest, origin in mapping.items() if origin in df.columns}
    return df.rename(columns=reverse)


def _validate_columns(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            "Faltan columnas requeridas en listings: "
            f"{sorted(missing)}. Si este CSV es de INE/IPV (Periodo/Índice), configúralo en historical.sources y no en paths.listings_csv."
        )


def _parse_quarter_to_date(value: str) -> pd.Timestamp | None:
    match = QUARTER_RE.match(str(value).strip())
    if not match:
        return None
    year = int(match.group(1))
    quarter = int(match.group(2))
    return pd.Timestamp(year=year, month=quarter * 3, day=1)


def _is_ine_periodic_csv(df: pd.DataFrame) -> bool:
    normalized = {str(c).strip().lower() for c in df.columns}
    has_period = any("periodo" in c or c == "period" for c in normalized)
    has_index_dim = any("indices y tasas" in c or "índices y tasas" in c for c in normalized)
    return has_period and has_index_dim


def _build_proxy_listings_from_ine(df: pd.DataFrame, cfg: dict | None = None) -> pd.DataFrame:
    norm_map = {_normalize_text(c): c for c in df.columns}
    period_col = next((norm_map[k] for k in norm_map if "periodo" in k or k == "period"), None)
    if period_col is None:
        raise ValueError("CSV INE sin columna de periodo para crear proxy de listings.")

    filtered = df.copy()
    default_selector = {
        "total_nacional": "Nacional",
        "comunidades_y_ciudades_autonomas": "",
        "general_vivienda_nueva_y_de_segunda_mano": "General",
        "indices_y_tasas": "Índice",
    }
    selector = ((cfg or {}).get("historical", {}).get("sources", [{}])[0].get("selector") or default_selector)
    for raw_key, expected in selector.items():
        col = norm_map.get(_normalize_text(raw_key))
        if not col:
            continue
        if str(expected).strip() == "":
            filtered = filtered[filtered[col].isna() | filtered[col].astype(str).str.strip().eq("")]
            continue
        expected_norm = _normalize_text(expected)
        filtered = filtered[filtered[col].astype(str).apply(_normalize_text).eq(expected_norm)]

    if filtered.empty:
        filtered = df.copy()

    value_col = "Total" if "Total" in filtered.columns else next((c for c in filtered.columns if c != period_col), None)
    if value_col is None:
        raise ValueError("CSV INE sin columna de valor para crear proxy de listings.")

    tmp = filtered[[period_col, value_col]].copy()
    tmp.columns = ["period_raw", "series_value_raw"]
    tmp["date"] = tmp["period_raw"].apply(_parse_quarter_to_date)
    tmp["series_value"] = (
        tmp["series_value_raw"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    tmp["series_value"] = pd.to_numeric(tmp["series_value"], errors="coerce")
    tmp = tmp.dropna(subset=["date", "series_value"]).sort_values("date")
    if tmp.empty:
        raise ValueError("No se pudieron parsear valores numéricos del CSV INE para crear listings proxy.")

    municipios = (cfg or {}).get("filters", {}).get("municipios_prioritarios", []) or ["Velilla de San Antonio"]
    base_m2 = 90.0
    rows: list[dict[str, Any]] = []
    for i, (_, row) in enumerate(tmp.iterrows()):
        ppsm = float(row["series_value"]) * 20.0
        for j, municipio in enumerate(municipios):
            m2 = base_m2 + ((i + j) % 6) * 5
            rows.append(
                {
                    "listing_id": f"INE_PROXY_{i:04d}_{j:02d}",
                    "date": row["date"],
                    "municipio": municipio,
                    "price": round(ppsm * m2, 2),
                    "m2": m2,
                    "rooms": 2 + ((i + j) % 3),
                    "garage": 1,
                    "ascensor": 1,
                    "piscina": 1 if (i + j) % 2 == 0 else 0,
                    "zonas_comunes": 1,
                    "tipologia": "media",
                }
            )
    return pd.DataFrame(rows)


def load_listings_csv(path: str, cfg: dict | None = None) -> pd.DataFrame:
    resolved_path = resolve_data_path(path)
    df = _read_csv_resilient(str(resolved_path))
    try:
        _validate_columns(df)
    except ValueError:
        if _is_ine_periodic_csv(df):
            return _build_proxy_listings_from_ine(df, cfg=cfg)
        raise
    df["date"] = pd.to_datetime(df["date"])
    return _ensure_tipologia(df)


def load_listings_api(api_cfg: dict[str, Any]) -> pd.DataFrame:
    endpoint = api_cfg["endpoint"]
    method = api_cfg.get("method", "GET").upper()
    timeout = int(api_cfg.get("timeout_seconds", 25))

    headers = dict(api_cfg.get("headers", {}))
    token = api_cfg.get("token")
    token_header = api_cfg.get("token_header", "Authorization")
    if token:
        headers[token_header] = token if token_header.lower() != "authorization" else f"Bearer {token}"

    params = api_cfg.get("params") or {}
    payload = api_cfg.get("payload") if method in {"POST", "PUT"} else None

    resp = requests.request(method=method, url=endpoint, headers=headers, params=params, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    records_key = api_cfg.get("records_key")
    records = data.get(records_key, data) if isinstance(data, dict) else data
    if not isinstance(records, list):
        raise ValueError("La API de listings no devolvió una lista de registros")

    df = pd.DataFrame(records)
    df = _normalize_columns(df, api_cfg.get("field_mapping"))
    _validate_columns(df)

    df["date"] = pd.to_datetime(df["date"])
    numeric_cols = ["price", "m2", "rooms", "garage", "ascensor"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    opt_cols = ["piscina", "zonas_comunes"]
    for col in opt_cols:
        if col not in df.columns:
            df[col] = 0

    return _ensure_tipologia(df)


def load_listings(cfg: dict[str, Any]) -> pd.DataFrame:
    source = cfg["listings"].get("source", "csv")
    if source == "api":
        return load_listings_api(cfg["listings"]["api"])
    return load_listings_csv(cfg["paths"]["listings_csv"], cfg=cfg)
