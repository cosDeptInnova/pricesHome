from __future__ import annotations

from typing import Any

import pandas as pd
import requests


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
        raise ValueError(f"Faltan columnas requeridas en listings: {sorted(missing)}")


def load_listings_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    _validate_columns(df)
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
    return load_listings_csv(cfg["paths"]["listings_csv"])
