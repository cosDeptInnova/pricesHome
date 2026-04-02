from openai import OpenAI


def generate_briefing(cfg: dict, summary_payload: dict) -> str:
    oai = cfg.get("openai", {})
    if not oai.get("enabled", False):
        return "OpenAI desactivado en config. Briefing IA omitido."

    api_key = oai.get("api_key", "").strip()
    if not api_key or "PUT_YOUR_OPENAI_API_KEY_HERE" in api_key:
        return "API key OpenAI no configurada. Briefing IA omitido."

    client = OpenAI(api_key=api_key)
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
Sé concreto y accionable para primera vivienda en Madrid y Corredor del Henares.
"""

    resp = client.responses.create(model=model, input=prompt)
    return resp.output_text.strip()
