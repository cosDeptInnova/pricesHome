from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.logger import get_logger

logger = get_logger("research_dispatch")


def _base_dispatch_result(engine: str, prompt_count: int) -> dict[str, Any]:
    return {
        "engine": engine,
        "prompt_count": int(prompt_count),
        "published": False,
        "published_count": 0,
        "target": None,
        "status": "skipped",
        "message": "No dispatch ejecutado.",
    }


def dispatch_research_prompts(cfg: dict, prompts: list[dict]) -> dict[str, Any]:
    research_cfg = cfg.get("research", {})
    engine = research_cfg.get("engine", "thread_pool")
    result = _base_dispatch_result(engine=engine, prompt_count=len(prompts))

    if not prompts:
        result["message"] = "No hay prompts para publicar."
        return result

    if engine != "kafka":
        result["message"] = (
            "Dispatch omitido: el engine configurado no es kafka. "
            "Los prompts quedan en archivo JSON para consumo posterior."
        )
        return result

    kafka_cfg = research_cfg.get("kafka", {})
    bootstrap_servers = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    topic = kafka_cfg.get("topic", "homebuy.research.prompts")
    client_id = kafka_cfg.get("client_id", "homebuy-ai")
    timeout_ms = int(float(kafka_cfg.get("publish_timeout_s", 5)) * 1000)
    result["target"] = {"bootstrap_servers": bootstrap_servers, "topic": topic}

    try:
        from kafka import KafkaProducer
    except ImportError:
        msg = "kafka-python no está instalado; no se pudo publicar en Kafka."
        logger.warning(msg)
        result["status"] = "dependency_missing"
        result["message"] = msg
        return result

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=20,
        )

        for idx, prompt in enumerate(prompts):
            payload = {
                "event_type": "homebuy.research_prompt",
                "published_at_utc": datetime.now(timezone.utc).isoformat(),
                "position": idx,
                "total": len(prompts),
                "prompt": prompt,
            }
            future = producer.send(topic, value=payload)
            future.get(timeout=timeout_ms / 1000)

        producer.flush(timeout=timeout_ms / 1000)
        producer.close()

        result.update(
            {
                "published": True,
                "published_count": len(prompts),
                "status": "published",
                "message": f"Prompts publicados en Kafka topic={topic}",
            }
        )
        logger.info(result["message"])
        return result
    except Exception as exc:  # noqa: BLE001
        msg = f"No se pudo publicar en Kafka ({bootstrap_servers}/{topic}): {exc}"
        logger.warning(msg)
        result["status"] = "publish_failed"
        result["message"] = msg
        return result
