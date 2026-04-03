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
from src.logger import configure_logging, get_logger
from src.model import infer_fair_price_per_m2, train_price_model
from src.scoring import compute_buy_score

logger = get_logger("pipeline")


def run_pipeline(cfg: dict) -> dict:
    output_dir = Path(cfg["paths"]["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    log_file = configure_logging(output_dir / "logs")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    logger.info("run_id=%s | 1) Cargando datos", run_id)
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

    logger.info("run_id=%s | 2) Filtrando listings", run_id)
    listings_f = filter_listings(listings, cfg)
    if listings_f.empty:
        raise ValueError("No quedan listings tras filtros. Ajusta config/filters.")

    logger.info("run_id=%s | listings_filtrados=%s", run_id, len(listings_f))

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
    model_result = train_price_model(train_df, cfg)
    train_df["fair_price_per_m2"] = infer_fair_price_per_m2(train_df, model_result)

    logger.info("run_id=%s | 5) Scoring", run_id)
    scored = compute_buy_score(train_df, cfg)
    scored = scored.sort_values("buy_score_0_100", ascending=False)

    forecast_df = build_forecast_frame(scored, horizon_months=cfg.get("forecast", {}).get("horizon_months", 6))

    top = scored.head(10)
    top_path = output_dir / "top_opportunities.csv"
    scored_path = output_dir / "scored_all.csv"
    forecast_path = output_dir / "forecast_prices.csv"
    historical_path = output_dir / "historical_series.csv"
    trace_path = output_dir / "decision_trace.csv"

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
    available = [c for c in trace_cols if c in scored.columns]
    scored[available].to_csv(trace_path, index=False)

    segment_models = getattr(model_result, "segment_models", {}) or {}

    summary = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "log_file": str(log_file),
        "model_mae_price_per_m2": model_result.mae,
        "num_listings_scored": int(len(scored)),
        "avg_buy_score": float(scored["buy_score_0_100"].mean()),
        "segmented_models_trained": int(len(segment_models)),
        "top_recommendations": top[
            [
                "listing_id",
                "municipio",
                "tipologia",
                "price",
                "m2",
                "buy_score_0_100",
                "recommendation",
                "decision_rationale",
            ]
        ].to_dict(orient="records"),
        "macro": macro_df.iloc[0].to_dict(),
        "news": news_df.iloc[0].to_dict(),
        "historical_features": historical_features,
    }

    brief = generate_briefing(cfg, summary)
    briefing_path = output_dir / "daily_briefing.txt"
    briefing_path.write_text(brief, encoding="utf-8")

    summary_path = output_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    logger.info("run_id=%s | Pipeline OK. Salidas en %s", run_id, output_dir)
    return {
        "summary": summary,
        "briefing": brief,
        "top_csv": str(top_path),
        "all_csv": str(scored_path),
        "forecast_csv": str(forecast_path),
        "historical_csv": str(historical_path),
        "trace_csv": str(trace_path),
        "briefing_txt": str(briefing_path),
        "summary_json": str(summary_path),
        "log_file": str(log_file),
    }
