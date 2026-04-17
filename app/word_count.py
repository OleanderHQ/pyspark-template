from __future__ import annotations

import os
import re
from collections import Counter
from typing import NamedTuple

STREAM_KEY = os.getenv("PUBLIC_STREAM_KAFKA_TOPIC", "public-stream")


class StreamMessage(NamedTuple):
    id: str
    body: str
    word_count: int
    created_at: str
    source: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    latitude: float | None
    longitude: float | None
    city: str | None
    country: str | None


def count_words(text: str) -> int:
    return len(text.split())


def tokenize(text: str) -> list[str]:
    return [
        t
        for t in (re.sub(r"[^a-zA-Z]", "", w).lower() for w in text.split())
        if len(t) >= 2
    ]


def coerce_messages(rows: list) -> list[StreamMessage]:
    return [
        StreamMessage(
            id=row.id,
            body=row.body,
            word_count=row.word_count,
            created_at=row.created_at,
            source=row.source,
            kafka_topic=row.kafka_topic,
            kafka_partition=row.kafka_partition,
            kafka_offset=row.kafka_offset,
            latitude=row.latitude,
            longitude=row.longitude,
            city=row.city,
            country=row.country,
        )
        for row in rows
    ]


def build_batch_word_deltas(batch_df) -> dict[str, int]:
    """Query the batch temp view for per-word counts.

    The caller must register the batch as ``__batch_messages`` before
    invoking this function.  Uses dict-key access for ``count`` to
    avoid collision with ``Row.count`` / ``dict.count``.
    """
    rows = batch_df.sparkSession.sql(
        "SELECT lower(word) AS word, COUNT(*) AS count "
        "FROM ("
        "  SELECT explode(split(body, '\\\\s+')) AS word"
        "  FROM __batch_messages"
        ") "
        "WHERE LENGTH(word) >= 2 "
        "GROUP BY lower(word)"
    ).collect()
    return {row.word: row["count"] for row in rows}


def summarize_inserted_messages(messages: list[StreamMessage]) -> dict:
    word_counts: Counter[str] = Counter()
    for msg in messages:
        word_counts.update(tokenize(msg.body))

    return {
        "stream_key": STREAM_KEY,
        "message_count": len(messages),
        "total_word_delta": sum(msg.word_count for msg in messages),
        "latest_message_id": messages[-1].id if messages else None,
        "longest_message_word_count": max(
            (msg.word_count for msg in messages), default=0
        ),
        "word_counts": dict(word_counts),
    }
