# HomeBuy AI

Sistema para estimar ventana de compra de primera vivienda en Comunidad de Madrid y Corredor del Henares.

## 1. Instalación

```bash
python -m venv .venv
source .venv/bin/activate  # en Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 2. Configuración

Edita `config/config.yaml`:

- `listings.source`: `csv` o `api`.
- `historical.sources`: soporta **CSV y XLSX** (incluyendo el fichero `../historico_vivienda_INE.xlsx`).
- filtros de vivienda.
- métricas macro.
- feeds RSS.

## 3. Ejecutar pipeline

```bash
python run.py
```

Salidas en `data/output/`:

- `scored_all.csv`
- `top_opportunities.csv`
- `forecast_prices.csv`
- `historical_series.csv` (si hay históricos)
- `decision_trace.csv` (**explicabilidad por listing**)
- `summary.json`
- `daily_briefing.txt`
- `logs/homebuy_ai_*.log` (**trazabilidad completa de ejecución**)

## 4. Dashboard

```bash
streamlit run app.py
```

Incluye:

- Top oportunidades de compra con filtros interactivos.
- Gráfico predictivo por municipio + tipología.
- Trazabilidad de scoring (componentes del score y racional de decisión).
- Insights de series históricas cargadas (CSV/XLSX).
- Briefing IA accionable.

## 5. Enfoque polieédrico implementado

El score y la predicción combinan múltiples caras del problema:

- **Mercado listing a listing**: precio, m2, habitaciones, equipamientos.
- **Macro**: euríbor, inflación, paro, renta variable.
- **Señal mediática**: intensidad y tono de noticias económicas/inmobiliarias.
- **Históricos oficiales**: series abiertas INE/MITMA/BdE + fichero Excel INE.
- **Modelado segmentado**: modelos por `municipio + tipología` cuando hay suficiente muestra.

Esto permite pasar de una foto puntual a una decisión de compra más robusta y con contexto temporal.
