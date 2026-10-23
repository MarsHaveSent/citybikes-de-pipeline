from datetime import UTC, datetime

from pyspark.sql import functions as F

from spark_jobs.station_status import LOG_SCHEMA, merge, to_silver, watermark

# Время с явным UTC
T1 = datetime(2026, 10, 20, 23, 50, tzinfo=UTC)
T2 = datetime(2026, 10, 21, 0, 0, tzinfo=UTC)
T3 = datetime(2026, 10, 21, 0, 10, tzinfo=UTC)


def batch(spark, *rows):
    return to_silver(spark.createDataFrame(list(rows), LOG_SCHEMA))


def test_watermark_of_missing_table_is_none(spark, tmp_path):
    assert watermark(spark, str(tmp_path / "station_status")) is None


def test_overlapping_batches_do_not_duplicate_rows(spark, tmp_path):
    path = str(tmp_path / "station_status")
    merge(spark, batch(spark, ("bubi", "1", T1, 3, 5, 0), ("bubi", "1", T2, 2, 6, 0)), path, False)

    # Следующий запуск перечитывает последний час
    merge(spark, batch(spark, ("bubi", "1", T2, 2, 6, 0), ("bubi", "1", T3, 1, 7, 0)), path, True)

    rows = (
        spark.read.format("delta")
        .load(path)
        .select(
            F.date_format("observed_at", "HH:mm").alias("at"),
            F.col("observed_date").cast("string").alias("day"),
            "free_bikes",
        )
        .orderBy("observed_at")
        .collect()
    )
    assert [tuple(r) for r in rows] == [
        ("23:50", "2026-10-20", 3),
        ("00:00", "2026-10-21", 2),
        ("00:10", "2026-10-21", 1),
    ]
    assert watermark(spark, path).astimezone(UTC) == T3


def test_empty_batch_changes_nothing(spark, tmp_path):
    path = str(tmp_path / "station_status")
    merge(spark, batch(spark, ("bubi", "1", T1, 3, 5, 0)), path, False)

    merge(spark, batch(spark), path, True)

    assert spark.read.format("delta").load(path).count() == 1
