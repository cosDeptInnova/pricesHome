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

### Validar uso de internet y OpenAI

Si dudas de si se están usando fuentes online o LLM:

- Revisa `data/output/logs/homebuy_ai_*.log`:
  - `Feed leído: ... | entradas=N` confirma lectura de RSS.
  - `OpenAI habilitado pero sin API key válida` indica que **no** se llamó al LLM.
  - `Generando briefing con OpenAI model=...` confirma llamada al LLM.
- Revisa `data/output/summary.json`:
  - `internet_news_ok=true/false`
  - `openai_briefing_expected=true/false`

> Nota: con `openai.api_key: "PUT_YOUR_OPENAI_API_KEY_HERE"` el briefing IA se omite por diseño.

Salidas en `data/output/`:

- `scored_all.csv`
- `top_opportunities.csv`
- `forecast_prices.csv`
- `historical_series.csv` (si hay históricos)
- `decision_trace.csv` (**explicabilidad por listing**)
- `summary.json`
- `daily_briefing.txt`
- `index_modifications.csv` (variaciones por trimestre/año detectadas en series históricas)
- `research_prompts.json` (prompts de búsqueda en internet para enriquecer la decisión)
- `logs/homebuy_ai_*.log` (**trazabilidad completa de ejecución**)
- `logs/homebuy_ai_*_events.jsonl` (**eventos estructurados JSONL paso a paso**)
- `summary.json > research_dispatch` (estado de envío de prompts a Kafka)

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


## 6. Investigación asistida por variaciones de índice


### Publicar prompts en Kafka (opcional)

Si configuras `research.engine: "kafka"`, el pipeline intentará publicar cada prompt en el topic configurado (por defecto `homebuy.research.prompts` en `localhost:9092`).

Requisitos:

```bash
pip install kafka-python
```

Si Kafka no está disponible o falta dependencia, la ejecución **no falla**: se conserva `research_prompts.json` y se registra el estado en `summary.json > research_dispatch`.

### CSV INE (encoding/parseo)

El loader ahora intenta codificaciones `utf-8`, `utf-8-sig`, `cp1252` y `latin-1`, además de separadores `;`, tab y `,`.

- Si usas `INE_Vivienda.csv` (formato INE con `Periodo` + `Índice`), configúralo en `historical.sources`.
- Si ese CSV se coloca por error en `paths.listings_csv`, el sistema devuelve un error explicativo indicando que no es un fichero de listings.


El pipeline ahora detecta variaciones históricas de índices (trimestrales/anuales) y crea prompts de búsqueda para investigación online:

- Detecta cambios por `indicator + scope` en `index_modifications.csv`.
- Prioriza las variaciones con mayor impacto relativo.
- Genera consultas en `research_prompts.json` para que un agente/LLM consulte contexto económico y ayude en la decisión de compra.

Además, se define un plan de orquestación (`research_orchestration`) configurable en `config/config.yaml`:

- `thread_pool` (MVP recomendado para herramienta funcional).
- `queue` (cola de trabajos con reintentos).
- `kafka` / `flink` (escalado avanzado cuando haya alto volumen continuo).
