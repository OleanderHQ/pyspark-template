from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.word_count import (
    STREAM_KEY,
    StreamMessage,
    count_words,
    coerce_messages,
    compute_sentiment,
    build_batch_word_deltas,
    summarize_inserted_messages,
    tokenize,
)
from entrypoint import _iceberg_rewrite_data_files_sql, _should_compact_batch


class PublicStreamSparkTests(unittest.TestCase):
    def test_should_compact_batch_uses_completed_batch_interval(self) -> None:
        self.assertFalse(_should_compact_batch(batch_id=0, interval_batches=0))
        self.assertFalse(_should_compact_batch(batch_id=3, interval_batches=5))
        self.assertTrue(_should_compact_batch(batch_id=4, interval_batches=5))

    def test_iceberg_rewrite_data_files_sql_targets_table_namespace(self) -> None:
        sql = _iceberg_rewrite_data_files_sql(
            "oleander.default.public_stream_messages",
            134_217_728,
        )

        self.assertEqual(
            sql,
            "CALL oleander.system.rewrite_data_files("
            "table => 'default.public_stream_messages', "
            "strategy => 'binpack', "
            "options => map('target-file-size-bytes', '134217728')"
            ")",
        )

    def test_count_words_handles_apostrophes_and_dashes(self) -> None:
        self.assertEqual(count_words("Oleander's stream keeps-on flowing"), 4)

    def test_tokenize_filters_short_tokens_and_normalizes(self) -> None:
        self.assertEqual(
            tokenize("Hi, hi! A stream of Words and words."),
            ["hi", "hi", "stream", "of", "words", "and", "words"],
        )

    def test_compute_sentiment_returns_normalized_score(self) -> None:
        self.assertGreater(compute_sentiment("I love this so much!"), 0.5)
        self.assertLess(compute_sentiment("This is terrible and awful."), 0.5)
        score = compute_sentiment("the cat sat on the mat")
        self.assertAlmostEqual(score, 0.5, delta=0.15)

    def test_coerce_messages_includes_kafka_metadata(self) -> None:
        rows = [
            SimpleNamespace(
                id="m1",
                body="hello spark world",
                word_count=3,
                created_at="2026-03-25T10:00:00Z",
                source="browser",
                kafka_topic="public-stream",
                kafka_partition=0,
                kafka_offset=12,
                latitude=37.7749,
                longitude=-122.4194,
                city="San Francisco",
                country="US",
            )
        ]

        messages = coerce_messages(rows)

        self.assertEqual(len(messages), 1)
        msg = messages[0]
        self.assertEqual(msg.id, "m1")
        self.assertEqual(msg.city, "San Francisco")
        self.assertIsInstance(msg.sentiment_score, float)
        self.assertGreaterEqual(msg.sentiment_score, 0.0)
        self.assertLessEqual(msg.sentiment_score, 1.0)

    def test_summarize_inserted_messages_builds_metrics_inputs(self) -> None:
        messages = [
            StreamMessage(
                id="m1",
                body="Spark loves words",
                word_count=3,
                created_at="2026-03-25T10:00:00Z",
                source="browser",
                kafka_topic="public-stream",
                kafka_partition=0,
                kafka_offset=1,
                latitude=51.5074,
                longitude=-0.1278,
                city="London",
                country="GB",
                sentiment_score=0.7,
            ),
            StreamMessage(
                id="m2",
                body="Words love Oleander",
                word_count=3,
                created_at="2026-03-25T10:00:01Z",
                source="browser",
                kafka_topic="public-stream",
                kafka_partition=0,
                kafka_offset=2,
                latitude=None,
                longitude=None,
                city=None,
                country=None,
                sentiment_score=0.6,
            ),
        ]

        summary = summarize_inserted_messages(messages)

        self.assertEqual(summary["stream_key"], STREAM_KEY)
        self.assertEqual(summary["message_count"], 2)
        self.assertEqual(summary["total_word_delta"], 6)
        self.assertEqual(summary["latest_message_id"], "m2")
        self.assertEqual(summary["longest_message_word_count"], 3)
        self.assertEqual(
            summary["word_counts"],
            {"spark": 1, "loves": 1, "words": 2, "love": 1, "oleander": 1},
        )

    def test_build_batch_word_deltas_uses_row_key_access(self) -> None:
        class FakeCollectedRow(dict):
            def __getattr__(self, name: str):
                if name == "count":
                    return dict.count
                return self[name]

        class FakeSparkSession:
            def sql(self, _query: str):
                return SimpleNamespace(
                    collect=lambda: [
                        FakeCollectedRow(word="spark", count=2),
                        FakeCollectedRow(word="oleander", count=1),
                    ]
                )

        fake_batch_df = SimpleNamespace(sparkSession=FakeSparkSession())

        self.assertEqual(
            build_batch_word_deltas(fake_batch_df),
            {"spark": 2, "oleander": 1},
        )


if __name__ == "__main__":
    unittest.main()
