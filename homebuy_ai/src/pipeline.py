from pathlib import Path
import json

from src.ai_briefing import generate_briefing
from src.data_sources.listings import load_listings_csv
from src.data_sources.macro import load_macro_snapshot
from src.data_sources.news import load_news_signals
from src.features import build_training_frame, filter_listings
from src.logger import get_logger
from src.model import infer_fair_price_per_m2, train_price_model
from src.scoring import compute_buy_score

logger = get_logger("pipeline")


def run_pipeline(cfg: dict) -> dict:
    output_dir = Path(cfg["paths"]["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("1) Cargando datos")
    listings = load_listings_csv(cfg["paths"]["listings_csv"])
    macro_df = load_macro_snapshot(cfg)
    news_df = load_news_signals(cfg)

    logger.info("2) Filtrando listings")
    listings_f = filter_listings(listings, cfg)
    if listings_f.empty:
        raise ValueError("No quedan listings tras filtros. Ajusta config/filters.")

    logger.info("3) Feature frame")
    train_df = build_training_frame(listings_f, macro_df, news_df)

    logger.info("4) Entrenando modelo")
    model_result = train_price_model(train_df, cfg)
    train_df["fair_price_per_m2"] = infer_fair_price_per_m2(train_df, model_result)

    logger.info("5) Scoring")
    scored = compute_buy_score(train_df, cfg)
    scored = scored.sort_values("buy_score_0_100", ascending=False)

    top = scored.head(10)
    top_path = output_dir / "top_opportunities.csv"
    scored_path = output_dir / "scored_all.csv"
    top.to_csv(top_path, index=False)
    scored.to_csv(scored_path, index=False)

    summary = {
        "model_mae_price_per_m2": model_result.mae,
        "num_listings_scored": int(len(scored)),
        "avg_buy_score": float(scored["buy_score_0_100"].mean()),
        "top_recommendations": top[
            [
                "listing_id",
                "municipio",
                "price",
                "m2",
                "buy_score_0_100",
                "recommendation",
            ]
        ].to_dict(orient="records"),
        "macro": macro_df.iloc[0].to_dict(),
        "news": news_df.iloc[0].to_dict(),
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
        "briefing_txt": str(briefing_path),
        "summary_json": str(summary_path),
    }
