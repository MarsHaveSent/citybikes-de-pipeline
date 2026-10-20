"""Одноразовый переход слоёв в HDFS с Parquet на Delta Lake.

Gold: CONVERT TO DELTA создаёт журнал для существующих parquet-файлов, не переписывая их,
и снимки прошлых дней становятся частью таблицы. Без этого первая запись Delta в каталог
создала бы новую таблицу, а старые снимки VACUUM удалил бы как файлы вне таблицы.
Silver удаляется: джоба пересобирает его из bronze целиком.

Повторный запуск ничего не меняет.
    spark-submit spark_jobs/convert_to_delta.py
"""

import logging

from pyspark.sql import SparkSession

log = logging.getLogger("citybikes.convert_to_delta")

SILVER_TABLES = ["/citybikes/silver/networks", "/citybikes/silver/stations"]
GOLD_TABLES = {"/citybikes/gold/stations_obt": "snapshot_date DATE"}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = SparkSession.builder.appName("citybikes_convert_to_delta").getOrCreate()
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

    def exists(path: str) -> bool:
        return fs.exists(jvm.org.apache.hadoop.fs.Path(path))

    def is_delta(path: str) -> bool:
        # Проверка по журналу, а не по папке _delta_log: после упавшей конвертации она бывает пустой
        return jvm.io.delta.tables.DeltaTable.isDeltaTable(spark._jsparkSession, path)

    try:
        for path in SILVER_TABLES:
            if exists(path) and not is_delta(path):
                fs.delete(jvm.org.apache.hadoop.fs.Path(path), True)
                log.info("%s: parquet удалён, silver пересоберётся", path)

        for path, partitioning in GOLD_TABLES.items():
            if not exists(path):
                continue
            if not is_delta(path):
                spark.sql(f"CONVERT TO DELTA parquet.`{path}` PARTITIONED BY ({partitioning})")
                log.info("%s: сконвертирован в Delta", path)
            snapshots = spark.read.format("delta").load(path).groupBy("snapshot_date").count()
            log.info("%s: снимки %s", path, sorted((str(d), n) for d, n in snapshots.collect()))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
