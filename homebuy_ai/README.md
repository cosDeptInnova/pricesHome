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

- `openai.api_key`
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
- `data/output/summary.json`
- `data/output/daily_briefing.txt`

## 4. Dashboard

```bash
streamlit run app.py
```

## 5. Siguientes mejoras

- Integrar API oficial de listings (en vez de CSV)
- Añadir series históricas (INE/MITMA/BdE)
- Modelos por municipio y tipología
- Alertas Telegram/Email
