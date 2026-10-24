"""Silver-слой: очищенные сети и станции из Postgres (bronze) в HDFS.

Подключение к Postgres - из переменных CITYBIKES_DB_HOST, _PORT, _NAME, _USER, _PASSWORD.
"""

import logging
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

log = logging.getLogger("citybikes.silver")

SILVER_PATH = "/citybikes/silver"
# Неделя версий для time travel, файлы старше VACUUM удаляет
RETENTION_HOURS = 168

LOCATION_SCHEMA = StructType(
    [
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ]
)

# Одно и то же поле в разных сетях приходит разными типами (renting - то true, то 1),
# поэтому скаляры читаются строками и приводятся явно
EXTRA_SCHEMA = StructType(
    [
        StructField(name, StringType())
        for name in (
            "renting",
            "returning",
            "last_updated",
            "slots",
            "ebikes",
            "has_ebikes",
            "payment-terminal",
            "address",
            "post_code",
        )
    ]
    + [StructField("payment", ArrayType(StringType()))]
)

# Вместо пустого значения источники присылают нулевое Unix-время 1970-01-01T00:00:00.
# Отсекается всё раньше 2000 года,настоящие даты в данных - с 2015 года
MIN_VALID_TIMESTAMP = "2000-01-01"


def clean_text(col: str | Column) -> Column:
    """trim; пустая строка становится NULL."""
    trimmed = F.trim(col)
    return F.when(trimmed != "", trimmed)


def parse_bool(col: str | Column) -> Column:
    value = F.lower(F.trim(col))
    return F.when(value.isin("true", "1"), True).when(value.isin("false", "0"), False)


def parse_timestamp(col: str | Column) -> Column:
    """Время из API -> timestamp UTC с точностью до секунды.

    Встречаются со смещением и без (без смещения считается UTC), пробел вместо T,
    лишняя Z после смещения ("+00:00Z") и Unix-время в секундах или миллисекундах.
    """
    value = F.regexp_replace(F.trim(col), r"([+-]\d{2}:\d{2})Z$", "$1")
    # В Spark 4: обычный cast на мусоре роняет джобу, try_* возвращают NULL
    parsed = (
        F.when(value.rlike(r"^\d{10}$"), F.timestamp_seconds(value.try_cast("bigint")))
        .when(value.rlike(r"^\d{13}$"), F.timestamp_millis(value.try_cast("bigint")))
        .otherwise(F.try_to_timestamp(value))
    )
    parsed = F.date_trunc("second", parsed)
    return F.when(parsed >= F.lit(MIN_VALID_TIMESTAMP).cast("timestamp"), parsed)


# Юридическая форма в конце названия: "Nextbike GmbH" и "nextbike GmbH" - один оператор
LEGAL_FORM = (
    r"(?i)[\s,]+(gmbh|inc|llc|ltd|limited|corp|corporation|co|company|ag|plc|oy|ab|a/s|as"
    r"|s\.?a|s\.?l|s\.?p\.?a|s\.?r\.?l|b\.?v|sas|sarl)\.?$"
)
# Варианты, которые не сводятся отбрасыванием формы, и названия с особым написанием.
# Ключ - название без формы в нижнем регистре, значение - как показывать на дашбордах
OPERATOR_NAMES = {
    "motivate international": "Motivate",
    "motivate": "Motivate",
    "pbsc urban solutions": "PBSC",
    "pbsc": "PBSC",
    "jcdecaux": "JCDecaux",
    "youbike": "YouBike",
    "docomo bike share": "DOCOMO BIKE SHARE",
    "bcycle": "BCycle",
}


def normalize_operator(col: str | Column) -> Column:
    """Название компании -> оператор для группировки; без названия - NULL."""
    name = F.trim(col)
    # Две формы подряд встречаются ("Co., Ltd."), поэтому проход повторяется
    for _ in range(2):
        name = F.regexp_replace(F.regexp_replace(name, LEGAL_FORM, ""), r"[\s,.]+$", "")
    key = F.lower(name)
    names = F.create_map(*[F.lit(x) for pair in OPERATOR_NAMES.items() for x in pair])
    return F.when(key != "", F.coalesce(names[key], F.initcap(key)))


def transform_networks(networks: DataFrame) -> DataFrame:
    location = F.col("location_parsed")
    companies = F.filter(F.transform("company", clean_text), lambda c: c.isNotNull())
    return (
        networks.withColumn("location_parsed", F.from_json("location", LOCATION_SCHEMA))
        .withColumn("companies", companies)
        .select(
            F.col("id").alias("network_id"),
            clean_text("name").alias("name"),
            clean_text(location.city).alias("city"),
            clean_text(location.country).alias("country"),
            location.latitude.alias("latitude"),
            location.longitude.alias("longitude"),
            "companies",
            F.coalesce(F.size("companies"), F.lit(0)).alias("companies_count"),
            # Оператор - первая из компаний сети; system - платформа проката, для справки
            normalize_operator(F.try_element_at("companies", F.lit(1))).alias("operator"),
            clean_text("system").alias("system"),
            clean_text("gbfs_href").alias("gbfs_href"),
            "ebikes",
            "scooters",
            "first_seen_at",
            "last_seen_at",
            "stations_synced_at",
        )
    )


def transform_stations(stations: DataFrame) -> DataFrame:
    extra = F.col("extra_parsed")
    return stations.withColumn("extra_parsed", F.from_json("extra", EXTRA_SCHEMA)).select(
        "network_id",
        "station_id",
        clean_text("name").alias("name"),
        "latitude",
        "longitude",
        parse_timestamp("timestamp").alias("timestamp"),
        "free_bikes",
        "empty_slots",
        parse_bool(extra.renting).alias("renting"),
        parse_bool(extra.returning).alias("returning"),
        parse_timestamp(extra.last_updated).alias("last_updated"),
        extra.slots.try_cast("int").alias("slots"),
        extra.ebikes.try_cast("int").alias("ebikes"),
        parse_bool(extra.has_ebikes).alias("has_ebikes"),
        extra.payment.alias("payment"),
        parse_bool(extra["payment-terminal"]).alias("payment_terminal"),
        clean_text(extra.address).alias("address"),
        clean_text(extra.post_code).alias("post_code"),
        "synced_at",
    )


def read_table(spark: SparkSession, table: str) -> DataFrame:
    env = os.environ
    url = (
        f"jdbc:postgresql://{env['CITYBIKES_DB_HOST']}:{env['CITYBIKES_DB_PORT']}"
        f"/{env['CITYBIKES_DB_NAME']}"
    )
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", env["CITYBIKES_DB_USER"])
        .option("password", env["CITYBIKES_DB_PASSWORD"])
        .load()
    )


def null_shares(df: DataFrame, columns: list[str]) -> dict[str, float]:
    total = df.count()
    if not total:
        return {}
    row = df.select([F.count_if(F.col(c).isNull()).alias(c) for c in columns]).first()
    return {c: round(row[c] / total, 3) for c in columns}


def write(spark: SparkSession, df: DataFrame, path: str, check_columns: list[str]) -> None:
    # Данных мало (~100 тыс. строк), один файл вместо файла на каждую партицию.
    # Silver целиком пересобирается из bronze, поэтому схема таблицы следует за кодом
    (
        df.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    spark.sql(f"VACUUM delta.`{path}` RETAIN {RETENTION_HOURS} HOURS")
    written = spark.read.format("delta").load(path)
    shares = null_shares(written, check_columns)
    log.info("%s: %d строк, доля NULL %s", path, written.count(), shares)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = (
        SparkSession.builder.appName("citybikes_silver")
        # Время без смещения в данных считается UTC
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        # Таблицы читаются отдельными запросами с разницей в секунды, без общей транзакции.
        # Станции без сети при этом не появятся: краулер синхронизирует только сети из networks,
        # а FK не даёт записать станцию без сети. Каждая строка несёт своё время синхронизации
        networks = transform_networks(read_table(spark, "networks"))
        stations = transform_stations(read_table(spark, "stations"))
        write(spark, networks, f"{SILVER_PATH}/networks", ["city", "latitude", "system"])
        write(
            spark,
            stations,
            f"{SILVER_PATH}/stations",
            ["timestamp", "last_updated", "renting", "free_bikes", "address"],
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
