"""Silver-слой: история состояний станций из лога изменений (bronze station_status_log).

Загрузка инкрементальная: каждый запуск читает строки новее последней загруженной минус
LATE_ROWS_MARGIN и добавляет их через MERGE по ключу.
Первый запуск читает всю историю параллельно, нарезая период на диапазоны observed_at.

Подключение к Postgres - из переменных CITYBIKES_DB_HOST, _PORT, _NAME, _USER, _PASSWORD.
"""

import logging
import os
from datetime import datetime, timedelta

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("citybikes.station_status")

STATUS_PATH = "/citybikes/silver/station_status"
# Строки лога пишутся транзакцией на сеть, и запуск мог прочитать таблицу до коммита
# соседней сети с более ранним observed_at
LATE_ROWS_MARGIN = timedelta(hours=1)
BACKFILL_PARTITIONS = 8
RETENTION_HOURS = 168

LOG_SCHEMA = (
    "network_id string, station_id string, observed_at timestamp, "
    "free_bikes int, empty_slots int, ebikes int"
)
COLUMNS = [c.split()[0] for c in LOG_SCHEMA.split(", ")]
BOUNDS_QUERY = "SELECT min(observed_at) AS lo, max(observed_at) AS hi FROM station_status_log"


def jdbc_options() -> dict[str, str]:
    env = os.environ
    return {
        "url": f"jdbc:postgresql://{env['CITYBIKES_DB_HOST']}:{env['CITYBIKES_DB_PORT']}"
        f"/{env['CITYBIKES_DB_NAME']}",
        "user": env["CITYBIKES_DB_USER"],
        "password": env["CITYBIKES_DB_PASSWORD"],
    }


def read_log(spark: SparkSession, since: datetime | None) -> DataFrame:
    options = jdbc_options()
    if since is not None:
        df = (
            spark.read.format("jdbc")
            .options(**options, dbtable="station_status_log")
            .load()
            .filter(F.col("observed_at") > F.lit(since))
        )
    else:
        bounds = spark.read.format("jdbc").options(**options, query=BOUNDS_QUERY).load().first()
        if bounds.lo is None:
            return spark.createDataFrame([], LOG_SCHEMA)
        df = (
            spark.read.format("jdbc")
            .options(
                **options,
                dbtable="station_status_log",
                partitionColumn="observed_at",
                lowerBound=str(bounds.lo),
                upperBound=str(bounds.hi),
                numPartitions=str(BACKFILL_PARTITIONS),
            )
            .load()
        )
    return df.select(*COLUMNS)


def to_silver(log_rows: DataFrame) -> DataFrame:
    return log_rows.withColumn("observed_date", F.to_date("observed_at"))


def watermark(spark: SparkSession, path: str) -> datetime | None:
    """Последний загруженный observed_at; None, если таблицы ещё нет или она пуста."""
    try:
        return spark.read.format("delta").load(path).agg(F.max("observed_at")).first()[0]
    except AnalysisException:
        return None


def merge(spark: SparkSession, batch: DataFrame, path: str, table_exists: bool) -> None:
    if not table_exists:
        batch.write.format("delta").mode("append").partitionBy("observed_date").save(path)
        return
    first_day = batch.agg(F.min("observed_date")).first()[0]
    if first_day is None:
        return
    batch.createOrReplaceTempView("status_batch")
    # Строки лога не меняются, поэтому только вставка новых. Условие на observed_date
    # ограничивает поиск совпадений партициями последних дней, а не всей историей
    spark.sql(
        f"""
        MERGE INTO delta.`{path}` t
        USING status_batch s
        ON t.observed_date >= DATE'{first_day}'
           AND t.network_id = s.network_id AND t.station_id = s.station_id
           AND t.observed_at = s.observed_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = (
        SparkSession.builder.appName("citybikes_station_status")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        loaded = watermark(spark, STATUS_PATH)
        since = loaded - LATE_ROWS_MARGIN if loaded else None
        batch = to_silver(read_log(spark, since)).cache()
        rows = batch.count()
        merge(
            spark,
            batch.coalesce(BACKFILL_PARTITIONS if since is None else 1),
            STATUS_PATH,
            table_exists=loaded is not None,
        )
        spark.sql(f"VACUUM delta.`{STATUS_PATH}` RETAIN {RETENTION_HOURS} HOURS")
        metrics = (
            spark.sql(f"DESCRIBE HISTORY delta.`{STATUS_PATH}` LIMIT 5")
            .filter(F.col("operation").isin("MERGE", "WRITE"))
            .select("operationMetrics")
            .first()
        )
        log.info(
            "%s: прочитано %d строк начиная с %s, метрики записи %s, загружено до %s",
            STATUS_PATH,
            rows,
            since or "начала истории",
            metrics[0] if metrics else None,
            watermark(spark, STATUS_PATH),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
