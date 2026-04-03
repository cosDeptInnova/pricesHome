from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="HomeBuy AI", layout="wide")
st.title("🏠 HomeBuy AI - Timing de compra")

out = Path("data/output")
scored_path = out / "scored_all.csv"
brief_path = out / "daily_briefing.txt"
forecast_path = out / "forecast_prices.csv"
historical_path = out / "historical_series.csv"
trace_path = out / "decision_trace.csv"
summary_path = out / "summary.json"

if not scored_path.exists():
    st.warning("No hay resultados todavía. Ejecuta: python run.py")
    st.stop()

df = pd.read_csv(scored_path)
df["date"] = pd.to_datetime(df["date"])

st.subheader("Estado de ejecución")
c1, c2, c3 = st.columns(3)
with c1:
    st.metric("Listings analizados", len(df))
with c2:
    st.metric("Score medio", round(df["buy_score_0_100"].mean(), 2))
with c3:
    st.metric("BUY_WINDOW", int((df["recommendation"] == "BUY_WINDOW").sum()))

if summary_path.exists():
    st.caption(f"Resumen disponible: {summary_path}")
if trace_path.exists():
    st.caption(f"Traza de decisión: {trace_path}")

st.subheader("Filtros interactivos")
f1, f2, f3 = st.columns(3)
with f1:
    municipios = sorted(df["municipio"].dropna().unique().tolist())
    sel_municipios = st.multiselect("Municipios", municipios, default=municipios)
with f2:
    tipologias = sorted(df["tipologia"].dropna().unique().tolist())
    sel_tipologias = st.multiselect("Tipologías", tipologias, default=tipologias)
with f3:
    min_score, max_score = int(df["buy_score_0_100"].min()), int(df["buy_score_0_100"].max())
    score_range = st.slider("Rango score", min_score, max_score, (min_score, max_score))

filtered = df[
    df["municipio"].isin(sel_municipios)
    & df["tipologia"].isin(sel_tipologias)
    & df["buy_score_0_100"].between(score_range[0], score_range[1])
]

st.subheader("Top oportunidades")
st.dataframe(
    filtered.sort_values("buy_score_0_100", ascending=False)[
        [
            "listing_id",
            "municipio",
            "tipologia",
            "price",
            "m2",
            "price_per_m2",
            "fair_price_per_m2",
            "buy_score_0_100",
            "recommendation",
            "decision_rationale",
        ]
    ].head(30),
    use_container_width=True,
)

c1, c2 = st.columns(2)
with c1:
    st.subheader("Distribución score")
    st.bar_chart(filtered.set_index("listing_id")["buy_score_0_100"])

with c2:
    st.subheader("Gap valoración por municipio")
    gap = ((filtered["fair_price_per_m2"] - filtered["price_per_m2"]) / filtered["fair_price_per_m2"].clip(lower=1))
    gap_df = pd.DataFrame({"municipio": filtered["municipio"], "valuation_gap": gap})
    st.bar_chart(gap_df.groupby("municipio").mean(numeric_only=True))

st.subheader("Trazabilidad del scoring")
if trace_path.exists():
    trace = pd.read_csv(trace_path)
    st.dataframe(trace.head(50), use_container_width=True)
else:
    st.info("No hay traza de scoring todavía.")

st.subheader("Gráfico predictivo por municipio y tipología")
if forecast_path.exists():
    ff = pd.read_csv(forecast_path)
    ff["month"] = pd.to_datetime(ff["month"])

    municipios_f = sorted(ff["municipio"].unique().tolist())
    municipio_sel = st.selectbox("Municipio", municipios_f)
    tipologias_f = sorted(ff[ff["municipio"] == municipio_sel]["tipologia"].unique().tolist())
    tipologia_sel = st.selectbox("Tipología", tipologias_f)

    curve = ff[(ff["municipio"] == municipio_sel) & (ff["tipologia"] == tipologia_sel)].copy()
    chart_df = curve.pivot_table(index="month", columns="series", values="forecast_price_per_m2", aggfunc="mean")
    st.line_chart(chart_df)
else:
    st.info("No hay forecast aún. Ejecuta de nuevo el pipeline.")

st.subheader("Insights históricos abiertos")
if historical_path.exists():
    h = pd.read_csv(historical_path)
    h["date"] = pd.to_datetime(h["date"])
    st.dataframe(h.sort_values("date", ascending=False).head(30), use_container_width=True)

    last = h.sort_values("date").groupby(["indicator", "scope"], as_index=False).tail(1)
    st.caption("Último valor por indicador (INE / MITMA / BdE / Excel INE).")
    st.dataframe(last[["indicator", "scope", "date", "series_value"]], use_container_width=True)
else:
    st.info("Sin históricos. Añade CSV/XLSX en config.historical.sources.")

st.subheader("Briefing IA")
if brief_path.exists():
    st.text(brief_path.read_text(encoding="utf-8"))
else:
    st.info("No hay briefing generado aún.")
