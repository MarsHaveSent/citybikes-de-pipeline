"""Gold-слой: One Big Table станций с атрибутами сетей. Источник - silver в HDFS.

Витрина - срез инфраструктуры: где станции, вместимость, способы оплаты, электровелосипеды.
Ежедневные снимки в партициях snapshot_date показывают, как меняется сеть станций.
Счётчики (free_bikes, empty_slots, renting) - состояние на момент снимка, а он приходится
на разное местное время в разных регионах; спрос анализируется по витринам из лога изменений.
    spark-submit stations_obt.py 2026-10-18
"""

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("citybikes.stations_obt")

SILVER_PATH = "/citybikes/silver"
OBT_PATH = "/citybikes/gold/stations_obt"
RETENTION_HOURS = 168


def build_obt(networks: DataFrame, stations: DataFrame, snapshot_date: str) -> DataFrame:
    """Сети без станций остаются в витрине с пустыми полями станции."""
    n, s = networks.alias("n"), stations.alias("s")
    return n.join(s, on="network_id", how="left").select(
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
        "network_id",
        F.col("n.name").alias("network_name"),
        F.col("n.latitude").alias("network_latitude"),
        F.col("n.longitude").alias("network_longitude"),
        F.col("n.city").alias("network_city"),
        F.col("n.country").alias("network_country"),
        F.col("n.companies").alias("network_companies"),
        F.col("n.companies_count").alias("network_companies_count"),
        F.col("n.system").alias("network_system"),
        F.col("s.station_id").alias("station_id"),
        F.col("s.name").alias("station_name"),
        F.when(
            F.col("s.name").isNotNull(), F.concat_ws("/", F.col("n.name"), F.col("s.name"))
        ).alias("network_station"),
        F.col("s.latitude").alias("station_latitude"),
        F.col("s.longitude").alias("station_longitude"),
        F.col("s.timestamp").alias("station_timestamp"),
        F.col("s.free_bikes").alias("station_free_bikes"),
        F.col("s.empty_slots").alias("station_empty_slots"),
        F.col("s.renting").alias("station_renting"),
        F.col("s.returning").alias("station_returning"),
        F.col("s.last_updated").alias("station_last_updated"),
        F.col("s.slots").alias("station_slots"),
        F.col("s.ebikes").alias("station_ebikes"),
        F.col("s.has_ebikes").alias("station_has_ebikes"),
        F.col("s.payment").alias("station_payment"),
        F.col("s.payment_terminal").alias("station_payment_terminal"),
        F.col("s.address").alias("station_address"),
        F.col("s.post_code").alias("station_post_code"),
        F.col("s.synced_at").alias("station_synced_at"),
    )


def write_snapshot(obt: DataFrame, path: str, snapshot_date: str) -> None:
    # replaceWhere: overwrite заменяет только снимок за эту дату, прошлые остаются.
    # Строку с другой датой Delta не запишет, а без условия overwrite удалил бы всю витрину
    (
        obt.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_date = DATE'{snapshot_date}'")
        .partitionBy("snapshot_date")
        .save(path)
    )


def main() -> None:
    snapshot_date = sys.argv[1]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = (
        SparkSession.builder.appName("citybikes_stations_obt")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        obt = build_obt(
            spark.read.format("delta").load(f"{SILVER_PATH}/networks"),
            spark.read.format("delta").load(f"{SILVER_PATH}/stations"),
            snapshot_date,
        )
        write_snapshot(obt, OBT_PATH, snapshot_date)
        # Удаляет только файлы, заменённые перезапусками за ту же дату
        spark.sql(f"VACUUM delta.`{OBT_PATH}` RETAIN {RETENTION_HOURS} HOURS")
        written = (
            spark.read.format("delta")
            .load(OBT_PATH)
            .filter(F.col("snapshot_date") == snapshot_date)
        )
        log.info(
            "%s, снимок %s: %d строк, сетей без станций: %d",
            OBT_PATH,
            snapshot_date,
            written.count(),
            written.filter(F.col("station_id").isNull()).count(),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
