from pathlib import Path
import json

from src.ai_briefing import generate_briefing
from src.data_sources.historical import build_historical_features, load_historical_series
from src.data_sources.listings import load_listings
from src.data_sources.macro import load_macro_snapshot
from src.data_sources.news import load_news_signals
from src.features import build_training_frame, filter_listings
from src.forecast import build_forecast_frame
from src.logger import get_logger
from src.model import infer_fair_price_per_m2, train_price_model
from src.scoring import compute_buy_score

logger = get_logger("pipeline")


def run_pipeline(cfg: dict) -> dict:
    output_dir = Path(cfg["paths"]["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("1) Cargando datos")
    listings = load_listings(cfg)
    macro_df = load_macro_snapshot(cfg)
    news_df = load_news_signals(cfg)
    historical_df = load_historical_series(cfg)
    historical_features = build_historical_features(historical_df)

    logger.info("2) Filtrando listings")
    listings_f = filter_listings(listings, cfg)
    if listings_f.empty:
        raise ValueError("No quedan listings tras filtros. Ajusta config/filters.")

    logger.info("3) Feature frame")
    train_df = build_training_frame(listings_f, macro_df, news_df, historical_features)

    logger.info("4) Entrenando modelo")
    model_result = train_price_model(train_df, cfg)
    train_df["fair_price_per_m2"] = infer_fair_price_per_m2(train_df, model_result)

    logger.info("5) Scoring")
    scored = compute_buy_score(train_df, cfg)
    scored = scored.sort_values("buy_score_0_100", ascending=False)

    forecast_df = build_forecast_frame(scored, horizon_months=cfg.get("forecast", {}).get("horizon_months", 6))

    top = scored.head(10)
    top_path = output_dir / "top_opportunities.csv"
    scored_path = output_dir / "scored_all.csv"
    forecast_path = output_dir / "forecast_prices.csv"
    historical_path = output_dir / "historical_series.csv"

    top.to_csv(top_path, index=False)
    scored.to_csv(scored_path, index=False)
    forecast_df.to_csv(forecast_path, index=False)
    if not historical_df.empty:
        historical_df.to_csv(historical_path, index=False)

    summary = {
        "model_mae_price_per_m2": model_result.mae,
        "num_listings_scored": int(len(scored)),
        "avg_buy_score": float(scored["buy_score_0_100"].mean()),
        "segmented_models_trained": int(len(model_result.segment_models)),
        "top_recommendations": top[
            [
                "listing_id",
                "municipio",
                "tipologia",
                "price",
                "m2",
                "buy_score_0_100",
                "recommendation",
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

    logger.info(f"Pipeline OK. Salidas en {output_dir}")
    return {
        "summary": summary,
        "briefing": brief,
        "top_csv": str(top_path),
        "all_csv": str(scored_path),
        "forecast_csv": str(forecast_path),
        "historical_csv": str(historical_path),
        "briefing_txt": str(briefing_path),
        "summary_json": str(summary_path),
    }
