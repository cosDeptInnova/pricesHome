from datetime import datetime, timezone

import feedparser
import pandas as pd


def load_news_signals(cfg: dict) -> pd.DataFrame:
    feeds = cfg["news"]["rss_feeds"]
    keywords = [k.lower() for k in cfg["news"]["keywords"]]

    rows = []
    for url in feeds:
        parsed = feedparser.parse(url)
        for e in parsed.entries[:30]:
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
        return pd.DataFrame(
            [{"news_volume": 0, "news_keyword_ratio": 0.0, "news_sentiment": 0.0}]
        )

    df = pd.DataFrame(rows)
    volume = len(df)
    ratio = df["keyword_hit"].mean() if volume else 0.0
    sentiment = (ratio - 0.3) * 2.0
    sentiment = max(-1.0, min(1.0, sentiment))

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
