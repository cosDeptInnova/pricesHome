from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


@dataclass
class IndexModification:
    indicator: str
    scope: str
    period_type: str
    period_label: str
    previous_value: float
    current_value: float
    absolute_change: float
    pct_change: float


def _infer_period_type(series_dates: pd.Series) -> str:
    diffs = series_dates.sort_values().diff().dropna().dt.days
    if diffs.empty:
        return "year"
    median_days = float(diffs.median())
    if 70 <= median_days <= 110:
        return "quarter"
    if 320 <= median_days <= 410:
        return "year"
    return "mixed"


def build_index_modifications(historical_df: pd.DataFrame) -> list[IndexModification]:
    if historical_df.empty:
        return []

    required = {"indicator", "scope", "date", "series_value"}
    if not required.issubset(set(historical_df.columns)):
        return []

    mods: list[IndexModification] = []
    for (indicator, scope), group in historical_df.groupby(["indicator", "scope"]):
        g = group.sort_values("date").copy()
        period_type = _infer_period_type(g["date"])
        g["previous_value"] = g["series_value"].shift(1)
        g = g.dropna(subset=["previous_value"]) 
        if g.empty:
            continue

        g["absolute_change"] = g["series_value"] - g["previous_value"]
        diff = g["series_value"] - g["previous_value"]
        g["pct_change"] = np.where(g["previous_value"] == 0, 0.0, (diff / g["previous_value"]) * 100.0)

        for _, row in g.iterrows():
            dt = row["date"]
            if period_type == "quarter":
                period_label = f"{dt.year}-Q{((dt.month - 1) // 3) + 1}"
            else:
                period_label = str(dt.year)

            mods.append(
                IndexModification(
                    indicator=str(indicator),
                    scope=str(scope),
                    period_type=period_type,
                    period_label=period_label,
                    previous_value=float(row["previous_value"]),
                    current_value=float(row["series_value"]),
                    absolute_change=float(row["absolute_change"]),
                    pct_change=float(row["pct_change"]),
                )
            )

    return mods


def build_internet_search_prompts(mods: list[IndexModification], top_n: int = 12) -> list[dict]:
    if not mods:
        return []

    ranked = sorted(mods, key=lambda m: abs(m.pct_change), reverse=True)[:top_n]
    prompts = []
    for m in ranked:
        direction = "subida" if m.absolute_change >= 0 else "caída"
        prompt = (
            f"{m.indicator} {m.scope} {m.period_label} {direction} {m.pct_change:+.2f}% causas "
            "impacto compra vivienda primera residencia hipoteca euríbor"
        )
        prompts.append(
            {
                "indicator": m.indicator,
                "scope": m.scope,
                "period_type": m.period_type,
                "period_label": m.period_label,
                "pct_change": round(m.pct_change, 4),
                "absolute_change": round(m.absolute_change, 4),
                "search_prompt": prompt,
            }
        )
    return prompts


def build_orchestration_plan(cfg: dict, prompt_count: int) -> dict:
    research_cfg = cfg.get("research", {})
    engine = research_cfg.get("engine", "thread_pool")
    workers = int(research_cfg.get("max_workers", 4))

    if engine == "kafka":
        recommendation = "Aplicable: desacopla la generación de prompts y su consumo por agentes/retrievers en tiempo real."
    elif engine == "flink":
        recommendation = "Aplicable solo si procesas streaming continuo con ventanas y reglas complejas; para MVP diario no suele compensar."
    elif engine == "queue":
        recommendation = "Cola interna (Redis/RQ/Celery) para reintentos y control de throughput."
    else:
        recommendation = "Thread pool simple para MVP funcional con paralelización moderada."

    return {
        "engine": engine,
        "max_workers": max(1, workers),
        "prompt_count": int(prompt_count),
        "recommendation": recommendation,
    }


def modifications_to_frame(mods: list[IndexModification]) -> pd.DataFrame:
    if not mods:
        return pd.DataFrame(
            columns=[
                "indicator",
                "scope",
                "period_type",
                "period_label",
                "previous_value",
                "current_value",
                "absolute_change",
                "pct_change",
            ]
        )
    return pd.DataFrame([asdict(m) for m in mods])
