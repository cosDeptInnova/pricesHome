from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="HomeBuy AI", layout="wide")
st.title("🏠 HomeBuy AI - Timing de compra")

out = Path("data/output")
scored_path = out / "scored_all.csv"
brief_path = out / "daily_briefing.txt"

if not scored_path.exists():
    st.warning("No hay resultados todavía. Ejecuta: python run.py")
    st.stop()

df = pd.read_csv(scored_path)
st.metric("Listings analizados", len(df))
st.metric("Score medio", round(df["buy_score_0_100"].mean(), 2))

st.subheader("Top oportunidades")
st.dataframe(
    df.sort_values("buy_score_0_100", ascending=False)[
        [
            "listing_id",
            "municipio",
            "price",
            "m2",
            "price_per_m2",
            "fair_price_per_m2",
            "buy_score_0_100",
            "recommendation",
        ]
    ].head(20),
    use_container_width=True,
)

st.subheader("Distribución score")
st.bar_chart(df["buy_score_0_100"])

st.subheader("Briefing IA")
if brief_path.exists():
    st.text(brief_path.read_text(encoding="utf-8"))
else:
    st.info("No hay briefing generado aún.")
