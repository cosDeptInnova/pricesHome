from pathlib import Path
import inspect
import json
from datetime import datetime, timezone

from src.ai_briefing import generate_briefing
from src.data_sources.historical import build_historical_features, load_historical_series
import src.data_sources.listings as listings_source
from src.data_sources.macro import load_macro_snapshot
from src.data_sources.news import load_news_signals
from src.features import build_training_frame, filter_listings
from src.forecast import build_forecast_frame
from src.logger import JsonEventLogger, configure_logging, get_logger
from src.model import infer_fair_price_per_m2, train_price_model
from src.research_dispatch import dispatch_research_prompts
from src.research_prompts import (
    build_index_modifications,
    build_internet_search_prompts,
    build_orchestration_plan,
    modifications_to_frame,
)
from src.scoring import compute_buy_score

logger = get_logger("pipeline")




def _fill_missing_scored_columns(df):
    out = df.copy()

    if "tipologia" not in out.columns:
        fallback_col = next((c for c in ["tipo", "tipo_vivienda", "property_type"] if c in out.columns), None)
        out["tipologia"] = out[fallback_col].astype(str) if fallback_col else "media"

    defaults = {
        "listing_id": "unknown",
        "municipio": "desconocido",
        "price": 0.0,
        "m2": 0.0,
        "buy_score_0_100": 0.0,
        "recommendation": "NEUTRAL",
        "decision_rationale": "Sin trazabilidad disponible.",
    }
    for col, value in defaults.items():
        if col not in out.columns:
            out[col] = value

    return out


def run_pipeline(cfg: dict) -> dict:
    output_dir = Path(cfg["paths"]["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    log_file = configure_logging(output_dir / "logs")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_events_path = output_dir / "logs" / f"homebuy_ai_{run_id}_events.jsonl"
    events = JsonEventLogger(json_events_path)

    logger.info("run_id=%s | 1) Cargando datos", run_id)
    events.emit("pipeline_started", {"run_id": run_id})
    if hasattr(listings_source, "load_listings"):
        listings = listings_source.load_listings(cfg)
    else:
        logger.warning("load_listings no disponible; usando fallback load_listings_csv")
        listings = listings_source.load_listings_csv(cfg["paths"]["listings_csv"])
    macro_df = load_macro_snapshot(cfg)
    news_df = load_news_signals(cfg)
    historical_df = load_historical_series(cfg)
    historical_features = build_historical_features(historical_df)

    logger.info("run_id=%s | listings=%s historical_rows=%s", run_id, len(listings), len(historical_df))
    events.emit("data_loaded", {"listings": len(listings), "historical_rows": len(historical_df)})

    logger.info("run_id=%s | 2) Filtrando listings", run_id)
    listings_f = filter_listings(listings, cfg)
    if listings_f.empty:
        raise ValueError("No quedan listings tras filtros. Ajusta config/filters.")

    logger.info("run_id=%s | listings_filtrados=%s", run_id, len(listings_f))
    events.emit("listings_filtered", {"count": len(listings_f)})

    logger.info("run_id=%s | 3) Feature frame", run_id)
    build_frame_params = inspect.signature(build_training_frame).parameters
    if "historical_features" in build_frame_params:
        train_df = build_training_frame(
            listings_f,
            macro_df,
            news_df,
            historical_features=historical_features,
        )
    else:
        logger.warning(
            "build_training_frame sin soporte historical_features; continuando en modo compatibilidad"
        )
        train_df = build_training_frame(listings_f, macro_df, news_df)

    logger.info("run_id=%s | 4) Entrenando modelo", run_id)
    events.emit("training_started")
    model_result = train_price_model(train_df, cfg)
    train_df["fair_price_per_m2"] = infer_fair_price_per_m2(train_df, model_result)
    events.emit("training_completed", {"mae_price_per_m2": model_result.mae})

    logger.info("run_id=%s | 5) Scoring", run_id)
    scored = compute_buy_score(train_df, cfg)
    events.emit("scoring_completed", {"rows": len(scored)})
    scored = _fill_missing_scored_columns(scored)
    scored = scored.sort_values("buy_score_0_100", ascending=False)

    forecast_df = build_forecast_frame(scored, horizon_months=cfg.get("forecast", {}).get("horizon_months", 6))

    index_modifications = build_index_modifications(historical_df)
    research_prompts = build_internet_search_prompts(index_modifications, top_n=cfg.get("research", {}).get("top_prompts", 12))
    orchestration_plan = build_orchestration_plan(cfg, prompt_count=len(research_prompts))
    events.emit("research_prompts_built", {"count": len(research_prompts), "engine": orchestration_plan.get("engine")})
    dispatch_result = dispatch_research_prompts(cfg, research_prompts)
    events.emit("research_prompts_dispatch", dispatch_result)

    top = scored.head(10)
    top_path = output_dir / "top_opportunities.csv"
    scored_path = output_dir / "scored_all.csv"
    forecast_path = output_dir / "forecast_prices.csv"
    historical_path = output_dir / "historical_series.csv"
    trace_path = output_dir / "decision_trace.csv"
    index_mod_path = output_dir / "index_modifications.csv"
    prompts_path = output_dir / "research_prompts.json"

    top.to_csv(top_path, index=False)
    scored.to_csv(scored_path, index=False)
    forecast_df.to_csv(forecast_path, index=False)
    if not historical_df.empty:
        historical_df.to_csv(historical_path, index=False)

    trace_cols = [
        "listing_id",
        "municipio",
        "tipologia",
        "price",
        "m2",
        "price_per_m2",
        "fair_price_per_m2",
        "valuation_gap",
        "valuation_gap_component",
        "macro_affordability_component",
        "inventory_pressure_component",
        "news_sentiment_component",
        "buy_score_0_100",
        "recommendation",
        "decision_rationale",
    ]
    scored.reindex(columns=trace_cols).to_csv(trace_path, index=False)
    modifications_to_frame(index_modifications).to_csv(index_mod_path, index=False)
    prompts_path.write_text(json.dumps(research_prompts, ensure_ascii=False, indent=2), encoding="utf-8")

    segment_models = getattr(model_result, "segment_models", {}) or {}

    summary = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "log_file": str(log_file),
        "events_jsonl": str(json_events_path),
        "model_mae_price_per_m2": model_result.mae,
        "model_diagnostics": getattr(model_result, "diagnostics", {}),
        "num_listings_scored": int(len(scored)),
        "avg_buy_score": float(scored["buy_score_0_100"].mean()),
        "segmented_models_trained": int(len(segment_models)),
        "top_recommendations": top.reindex(
            columns=[
                "listing_id",
                "municipio",
                "tipologia",
                "price",
                "m2",
                "buy_score_0_100",
                "recommendation",
                "decision_rationale",
            ]
        ).to_dict(orient="records"),
        "macro": macro_df.iloc[0].to_dict(),
        "news": news_df.iloc[0].to_dict(),
        "historical_features": historical_features,
        "index_modifications_detected": len(index_modifications),
        "research_prompts": research_prompts,
        "research_orchestration": orchestration_plan,
        "research_dispatch": dispatch_result,
        "internet_news_ok": bool(news_df.iloc[0].get("news_volume", 0) > 0),
        "openai_briefing_expected": bool(
            cfg.get("openai", {}).get("enabled", False)
            and cfg.get("openai", {}).get("api_key", "").strip()
            and "PUT_YOUR_OPENAI_API_KEY_HERE" not in cfg.get("openai", {}).get("api_key", "")
        ),
    }

    brief = generate_briefing(cfg, summary)
    briefing_path = output_dir / "daily_briefing.txt"
    briefing_path.write_text(brief, encoding="utf-8")

    summary_path = output_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    logger.info("run_id=%s | Pipeline OK. Salidas en %s", run_id, output_dir)
    events.emit("pipeline_completed", {"output_dir": str(output_dir), "research_prompts": len(research_prompts)})
    return {
        "summary": summary,
        "briefing": brief,
        "top_csv": str(top_path),
        "all_csv": str(scored_path),
        "forecast_csv": str(forecast_path),
        "historical_csv": str(historical_path),
        "trace_csv": str(trace_path),
        "index_modifications_csv": str(index_mod_path),
        "research_prompts_json": str(prompts_path),
        "briefing_txt": str(briefing_path),
        "summary_json": str(summary_path),
        "log_file": str(log_file),
        "events_jsonl": str(json_events_path),
    }
