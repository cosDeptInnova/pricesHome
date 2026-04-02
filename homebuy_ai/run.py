from src.config_loader import load_config
from src.pipeline import run_pipeline

if __name__ == "__main__":
    cfg = load_config("config/config.yaml")
    result = run_pipeline(cfg)
    print("\n=== RESUMEN ===")
    print(f"Listings analizados: {result['summary']['num_listings_scored']}")
    print(f"MAE modelo (€/m2): {result['summary']['model_mae_price_per_m2']:.2f}")
    print(f"Score medio compra: {result['summary']['avg_buy_score']:.2f}")
    print(f"Top CSV: {result['top_csv']}")
    print(f"Briefing: {result['briefing_txt']}")
