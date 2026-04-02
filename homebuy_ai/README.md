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

- `listings.source`: `csv` o `api` (para integrar API oficial)
- `historical.sources`: series INE / MITMA / BdE (CSV)
- filtros de vivienda
- métricas macro
- feeds RSS

## 3. Ejecutar pipeline

```bash
python run.py
```

Salidas:

- `data/output/scored_all.csv`
- `data/output/top_opportunities.csv`
- `data/output/forecast_prices.csv`
- `data/output/historical_series.csv` (si hay históricos)
- `data/output/summary.json`
- `data/output/daily_briefing.txt`

## 4. Dashboard

```bash
streamlit run app.py
```

Incluye:

- Top oportunidades de compra
- Gráfico predictivo por municipio + tipología
- Insights de series históricas cargadas
- Briefing IA accionable

## 5. Carga manual de históricos (rápida)

Puedes descargar CSV de fuentes abiertas y cargarlos sin tocar código:

1. Crea un CSV por indicador con columnas:
   - `date` (YYYY-MM-DD)
   - `series_value` (numérico)
2. Guarda en `data/input/`.
3. Añade entrada en `historical.sources` dentro de `config.yaml`.

Ejemplo:

```yaml
historical:
  sources:
    - source: "INE"
      indicator: "ipc_yoy"
      scope: "Comunidad de Madrid"
      path: "data/input/historical_ine_ipc.csv"
```

## 6. Enfoque polieédrico implementado

El score y la predicción combinan múltiples caras del problema:

- **Mercado listing a listing**: precio, m2, habitaciones, equipamientos.
- **Macro**: euríbor, inflación, paro, renta variable.
- **Señal mediática**: intensidad y tono de noticias económicas/inmobiliarias.
- **Históricos oficiales**: series abiertas INE/MITMA/BdE.
- **Modelado segmentado**: modelos por `municipio + tipología` cuando hay suficiente muestra.

Esto permite pasar de una foto puntual a una decisión de compra más robusta y con contexto temporal.
