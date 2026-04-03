from openai import OpenAI


from src.logger import get_logger

logger = get_logger("ai_briefing")

def _fallback_briefing(summary_payload: dict) -> str:
    score = float(summary_payload.get("avg_buy_score", 0) or 0)
    rec = "esperar"
    if score >= 70:
        rec = "comprar ahora"
    elif score >= 45:
        rec = "negociar fuerte"
    return (
        "Briefing IA no disponible por incompatibilidad del cliente OpenAI en el entorno.\n"
        f"- Score medio: {score:.2f}\n"
        f"- Listings analizados: {summary_payload.get('num_listings_scored', 0)}\n"
        f"- Recomendación operativa: {rec}\n"
        "Acción: revisar dependencias openai/httpx (recomendado: openai>=1.40,<2 + httpx<0.28) o desactivar openai.enabled temporalmente."
    )


def generate_briefing(cfg: dict, summary_payload: dict) -> str:
    oai = cfg.get("openai", {})
    if not oai.get("enabled", False):
        logger.info("OpenAI briefing desactivado por configuración (openai.enabled=false)")
        return "OpenAI desactivado en config. Briefing IA omitido."

    api_key = oai.get("api_key", "").strip()
    if not api_key or "PUT_YOUR_OPENAI_API_KEY_HERE" in api_key:
        logger.warning("OpenAI habilitado pero sin API key válida; se omite briefing IA")
        return "API key OpenAI no configurada. Briefing IA omitido."

    model = oai.get("model", "gpt-4o-mini")

    prompt = f"""
Eres un analista inmobiliario cuantitativo.
Datos:
{summary_payload}

Genera:
1) Resumen ejecutivo (5-7 líneas)
2) Riesgos macro/geopolíticos clave
3) Señales a vigilar 30-90 días
4) Recomendación clara: comprar ahora / esperar / negociar fuerte
5) Explica brevemente qué pistas aportan los modelos (MAE vs baseline, MAE regresión lineal y top variables)
Sé concreto y accionable para primera vivienda en Madrid y Corredor del Henares.
"""

    try:
        client = OpenAI(api_key=api_key)
        logger.info("Generando briefing con OpenAI model=%s", model)
        resp = client.responses.create(model=model, input=prompt)
        return resp.output_text.strip()
    except TypeError as exc:
        if "proxies" in str(exc):
            logger.exception(
                "Incompatibilidad openai/httpx detectada (arg proxies). "
                "Suele resolverse fijando httpx<0.28 con openai 1.x: %s",
                exc,
            )
        else:
            logger.exception("Fallo de tipo generando briefing OpenAI; aplicando fallback local: %s", exc)
        return _fallback_briefing(summary_payload)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Fallo generando briefing OpenAI; aplicando fallback local: %s", exc)
        return _fallback_briefing(summary_payload)
