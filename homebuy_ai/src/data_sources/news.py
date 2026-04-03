from datetime import datetime, timezone

import feedparser
import pandas as pd

from src.logger import get_logger

logger = get_logger("news")


def load_news_signals(cfg: dict) -> pd.DataFrame:
    feeds = cfg["news"].get("rss_feeds", [])
    keywords = [k.lower() for k in cfg["news"].get("keywords", [])]

    rows = []
    for url in feeds:
        try:
            parsed = feedparser.parse(url)
            if getattr(parsed, "bozo", False):
                logger.warning("Feed con formato problemático: %s", url)
            entries = parsed.entries[:30]
            logger.info("Feed leído: %s | entradas=%s", url, len(entries))
        except Exception as ex:
            logger.warning("No se pudo leer feed %s: %s", url, ex)
            entries = []

        for e in entries:
            title = (e.get("title") or "").strip()
            summary = (e.get("summary") or "").strip()
            text = f"{title} {summary}".lower()
            hit = any(k in text for k in keywords)
            rows.append(
                {
                    "source": url,
                    "title": title,
                    "published": e.get("published", ""),
                    "keyword_hit": int(hit),
                }
            )

    if not rows:
        logger.warning("Sin noticias disponibles; se aplican señales neutras")
        return pd.DataFrame(
            [{"news_volume": 0, "news_keyword_ratio": 0.0, "news_sentiment": 0.0, "snapshot_utc": datetime.now(timezone.utc).isoformat()}]
        )

    df = pd.DataFrame(rows)
    volume = len(df)
    ratio = df["keyword_hit"].mean() if volume else 0.0
    sentiment = (ratio - 0.3) * 2.0
    sentiment = max(-1.0, min(1.0, sentiment))

    logger.info("Noticias procesadas=%s ratio_keywords=%.3f sentiment=%.3f", volume, ratio, sentiment)
    return pd.DataFrame(
        [
            {
                "news_volume": volume,
                "news_keyword_ratio": float(ratio),
                "news_sentiment": float(sentiment),
                "snapshot_utc": datetime.now(timezone.utc).isoformat(),
            }
        ]
    )
