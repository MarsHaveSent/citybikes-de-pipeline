"""Gold-слой: витрины спроса по местным суткам сети.

- station_daily: станция × местные сутки
- network_hourly: сеть × местный час, с погодой

Основа - интервалы состояния: значение станции действует от изменения до следующего.
Интервалы режутся по часам UTC, а потом каждому часу присваиваются местные дата и час сети.
Переход на зимнее/летнее время: два дня в году в местных сутках 25 или 23 часа, и только
в UTC каждый час уникален и длится ровно 3600 секунд.
Взятия и возвраты - уменьшения и увеличения free_bikes между соседними состояниями, это
оценка снизу: взятие и возврат на одной станции между опросами не видны. Изменение сразу
на REBALANCE_BIKES и больше за опрос - развоз оператора, оно идёт в operator_removed
и operator_added, а не в поездки.

    spark-submit demand.py [первая_дата последняя_дата]
Без дат считаются три последних местных дня; в любом случае только дни, которые у сети
уже закончились.
"""

import logging
import os
import sys
from datetime import UTC, date, datetime, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger("citybikes.demand")

STATUS_PATH = "/citybikes/silver/station_status"
SILVER_PATH = "/citybikes/silver"
GOLD_PATH = "/citybikes/gold"
EXPORT_PATH = "/citybikes/export"
DEFAULT_DAYS = 3
RETENTION_HOURS = 168
# Шаг частого опроса: по нему считается, какая часть суток наблюдалась
SLOT_SECONDS = 600
# Синхронизация засчитывается, только если данные у CityBikes свежее этого: сети Deutsche Bahn
# он обновляет раз в 20 минут, а днём их данные бывают заморожены по 14 часов
FRESH_SECONDS = 1800
# Пользователи меняют число велосипедов на станции за 10 минут на 1-2 (95% изменений), до 4
# взятия и возвраты встречаются одинаково часто. С 5 велосипедов привозов заметно больше,
# чем вывозов: это машина развоза
REBALANCE_BIKES = 5
# Если за один опрос изменилось больше половины станций сети, это сбой ленты или изменения
# за несколько часов, пришедшие разом
BURST_SHARE = 0.5
BURST_MIN_STATIONS = 20


def one_hour() -> Column:
    # Выражение, а не константа модуля: колонку Spark нельзя создать без активной сессии
    return F.expr("INTERVAL 1 HOUR")


def epoch(col: str | Column) -> Column:
    return F.unix_micros(col) / 1_000_000


def intervals(status: DataFrame, start: datetime, end: datetime) -> DataFrame:
    """Строки истории -> интервалы [valid_from, valid_to) внутри [start, end).

    В status должна быть и последняя строка каждой станции до start: она задаёт состояние
    на начало периода. Взятия и возвраты считаются только для изменений внутри периода
    и не считаются для массовых изменений (BURST_SHARE): состояние берётся, поездки - нет.
    """
    network_size = status.groupBy("network_id").agg(
        F.countDistinct("station_id").alias("network_stations")
    )
    bursts = (
        status.groupBy("network_id", "observed_at")
        .agg(F.count("*").alias("changed"))
        .join(network_size, "network_id")
        .filter(
            (F.col("changed") > BURST_SHARE * F.col("network_stations"))
            & (F.col("changed") >= BURST_MIN_STATIONS)
        )
        .select("network_id", "observed_at", F.lit(True).alias("burst"))
    )
    station = Window.partitionBy("network_id", "station_id").orderBy("observed_at")
    change = F.col("free_bikes") - F.lag("free_bikes").over(station)
    counted = (F.col("observed_at") >= F.lit(start)) & F.col("burst").isNull()
    by_users = F.abs(F.col("change")) < REBALANCE_BIKES
    down = F.greatest(-F.col("change"), F.lit(0))
    up = F.greatest(F.col("change"), F.lit(0))
    return (
        status.join(bursts, ["network_id", "observed_at"], "left")
        .withColumn("change", F.when(counted, change))
        .withColumn("next_at", F.lead("observed_at").over(station))
        .withColumn("valid_from", F.greatest("observed_at", F.lit(start)))
        .withColumn("valid_to", F.least(F.coalesce("next_at", F.lit(end)), F.lit(end)))
        .withColumn("pickups", F.when(by_users, down).otherwise(0))
        .withColumn("returns", F.when(by_users, up).otherwise(0))
        .withColumn("operator_removed", F.when(~by_users, down).otherwise(0))
        .withColumn("operator_added", F.when(~by_users, up).otherwise(0))
        .filter(F.col("valid_to") > F.col("valid_from"))
        .drop("burst")
    )


def station_hours(spans: DataFrame) -> DataFrame:
    """Интервалы -> станция × час UTC: секунды наблюдения, время пустой и полной станции.

    Часы UTC, а не местные: при переходе на зимнее время местный час 02:00 повторяется дважды.
    """
    last_hour = F.date_trunc("hour", F.col("valid_to") - F.expr("INTERVAL 1 MICROSECOND"))
    hours = F.sequence(F.date_trunc("hour", "valid_from"), last_hour, one_hour())
    pieces = spans.withColumn("hour_utc", F.explode(hours))
    seconds = epoch(F.least("valid_to", F.col("hour_utc") + one_hour())) - epoch(
        F.greatest("valid_from", "hour_utc")
    )
    pieces = pieces.withColumn("seconds", seconds)
    # Изменение относится к часу, в котором его увидели
    event_here = F.date_trunc("hour", "observed_at") == F.col("hour_utc")
    return pieces.groupBy("network_id", "station_id", "hour_utc").agg(
        F.sum("seconds").alias("seconds"),
        F.sum(F.col("free_bikes") * F.col("seconds")).alias("bike_seconds"),
        F.sum(F.col("ebikes") * F.col("seconds")).alias("ebike_seconds"),
        F.sum(F.when(F.col("free_bikes") == 0, F.col("seconds")).otherwise(0)).alias(
            "empty_seconds"
        ),
        # Число свободных мест сообщают не все сети
        F.sum(F.when(F.col("empty_slots") == 0, F.col("seconds"))).alias("full_seconds"),
        F.max(F.col("empty_slots").isNotNull()).alias("has_slots"),
        F.min("free_bikes").alias("min_free_bikes"),
        F.max("free_bikes").alias("max_free_bikes"),
        F.sum(F.when(event_here, F.col("pickups"))).alias("pickups"),
        F.sum(F.when(event_here, F.col("returns"))).alias("returns"),
        F.sum(F.when(event_here, F.col("operator_removed"))).alias("operator_removed"),
        F.sum(F.when(event_here, F.col("operator_added"))).alias("operator_added"),
        F.count(F.when(event_here & F.col("change").isNotNull(), 1)).alias("changes"),
    )


def localize(hourly: DataFrame, timezones: DataFrame) -> DataFrame:
    local = F.from_utc_timestamp("hour_utc", F.col("timezone"))
    return (
        hourly.join(timezones, "network_id")
        .withColumn("local_date", F.to_date(local))
        .withColumn("local_hour", F.hour(local))
    )


def coverage(syncs: DataFrame, timezones: DataFrame) -> DataFrame:
    """Доля 10-минутных слотов местного часа, в которых сеть синхронизировалась
    и получила свежие данные. Без времени обновления (старые синхронизации) данные считаются
    свежими."""
    slot = F.floor(epoch("synced_at") / SLOT_SECONDS)
    stale = epoch("synced_at") - epoch("feed_updated_at") > FRESH_SECONDS
    return (
        syncs.filter(~F.coalesce(stale, F.lit(False)))
        .withColumn("hour_utc", F.date_trunc("hour", "synced_at"))
        .groupBy("network_id", "hour_utc")
        .agg(F.countDistinct(slot).alias("slots"))
        .withColumn("observed_share", F.least(F.col("slots") / (3600 / SLOT_SECONDS), F.lit(1.0)))
        .transform(lambda df: localize(df, timezones))
    )


def local_midnight_utc(day: str | Column, timezone: str | Column) -> Column:
    """Местная полночь в UTC. Смещение берётся на эту дату: учитывает переход на зимнее/летнее
    время."""
    return F.to_utc_timestamp(F.to_timestamp(day), timezone)


def station_daily(hourly: DataFrame, hour_coverage: DataFrame) -> DataFrame:
    # Переход на зимнее/летнее время: в эти два дня сутки длятся 25 или 23 часа, поэтому
    # длина - между двумя местными полуночами, а не 24 часа
    day_seconds = epoch(local_midnight_utc(F.date_add("local_date", 1), F.col("timezone"))) - epoch(
        local_midnight_utc("local_date", F.col("timezone"))
    )
    day_coverage = (
        hour_coverage.withColumn("day_seconds", day_seconds)
        .groupBy("network_id", "local_date")
        .agg((F.sum("slots") * SLOT_SECONDS / F.first("day_seconds")).alias("slots_share"))
    )
    daily = hourly.groupBy("network_id", "station_id", "local_date").agg(
        F.coalesce(F.sum("pickups"), F.lit(0)).cast("int").alias("pickups"),
        F.coalesce(F.sum("returns"), F.lit(0)).cast("int").alias("returns"),
        F.coalesce(F.sum("operator_removed"), F.lit(0)).cast("int").alias("operator_removed"),
        F.coalesce(F.sum("operator_added"), F.lit(0)).cast("int").alias("operator_added"),
        F.sum("changes").cast("int").alias("changes"),
        (F.sum("bike_seconds") / F.sum("seconds")).alias("avg_free_bikes"),
        (F.sum("ebike_seconds") / F.sum("seconds")).alias("avg_ebikes"),
        F.min("min_free_bikes").alias("min_free_bikes"),
        F.max("max_free_bikes").alias("max_free_bikes"),
        (F.sum("empty_seconds") / 3600).alias("empty_hours"),
        F.when(F.max("has_slots"), F.coalesce(F.sum("full_seconds"), F.lit(0)) / 3600).alias(
            "full_hours"
        ),
    )
    return (
        daily.withColumn("net_flow", F.col("returns") - F.col("pickups"))
        .join(day_coverage, ["network_id", "local_date"], "left")
        .withColumn("observed_share", F.least(F.coalesce("slots_share", F.lit(0.0)), F.lit(1.0)))
        .drop("slots_share")
    )


def network_hourly(hourly: DataFrame, hour_coverage: DataFrame, weather: DataFrame) -> DataFrame:
    # Осадки Open-Meteo - сумма за час, который заканчивается в отметке времени:
    # часу 14:00-15:00 соответствует запись за 15:00
    weather = weather.withColumn("hour_utc", F.col("hour") - one_hour()).drop("hour", "fetched_at")
    grouped = hourly.groupBy("network_id", "hour_utc", "local_date", "local_hour").agg(
        F.countDistinct("station_id").alias("stations"),
        F.coalesce(F.sum("pickups"), F.lit(0)).cast("int").alias("pickups"),
        F.coalesce(F.sum("returns"), F.lit(0)).cast("int").alias("returns"),
        F.coalesce(F.sum("operator_removed"), F.lit(0)).cast("int").alias("operator_removed"),
        F.coalesce(F.sum("operator_added"), F.lit(0)).cast("int").alias("operator_added"),
        (F.sum("bike_seconds") / 3600).alias("bikes_available"),
        (F.sum("ebike_seconds") / 3600).alias("ebikes_available"),
        (F.sum("empty_seconds") / F.sum("seconds")).alias("empty_station_share"),
    )
    return (
        grouped.join(
            hour_coverage.select("network_id", "hour_utc", "observed_share"),
            ["network_id", "hour_utc"],
            "left",
        )
        .withColumn("observed_share", F.coalesce("observed_share", F.lit(0.0)))
        .join(weather, ["network_id", "hour_utc"], "left")
        .withColumn("weekday", F.dayofweek("local_date"))
        .withColumn("is_weekend", F.col("weekday").isin(1, 7))
    )


def with_network_attributes(df: DataFrame, networks: DataFrame) -> DataFrame:
    return df.join(
        networks.select(
            "network_id",
            F.col("city").alias("network_city"),
            F.col("country").alias("network_country"),
            F.col("operator").alias("network_operator"),
        ),
        "network_id",
        "left",
    )


def finished_days(df: DataFrame, first: date, last: date, now: datetime) -> DataFrame:
    """Дни периода, которые у сети уже закончились по местному времени."""
    day_end = local_midnight_utc(F.date_add("local_date", 1), F.col("timezone"))
    return df.filter(
        (F.col("local_date") >= F.lit(first))
        & (F.col("local_date") <= F.lit(last))
        & (day_end <= F.lit(now))
    )


def jdbc(spark: SparkSession, table: str) -> DataFrame:
    env = os.environ
    url = (
        f"jdbc:postgresql://{env['CITYBIKES_DB_HOST']}:{env['CITYBIKES_DB_PORT']}"
        f"/{env['CITYBIKES_DB_NAME']}"
    )
    return (
        spark.read.format("jdbc")
        .options(
            url=url,
            dbtable=table,
            user=env["CITYBIKES_DB_USER"],
            password=env["CITYBIKES_DB_PASSWORD"],
        )
        .load()
    )


def write_days(
    df: DataFrame,
    name: str,
    first: date,
    last: date,
    gold_path: str = GOLD_PATH,
    export_path: str = EXPORT_PATH,
) -> None:
    # Пересчитанные дни заменяются целиком; день, который у части сетей ещё не закончился,
    # дозапишется следующим запуском
    (
        df.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        # на BETWEEN при записи в существующую таблицу падает с INTERNAL_ERROR (unresolved object)
        .option("replaceWhere", f"local_date >= DATE'{first}' AND local_date <= DATE'{last}'")
        .option("mergeSchema", "true")
        .partitionBy("local_date")
        .save(f"{gold_path}/{name}")
    )
    df.coalesce(1).write.mode("overwrite").parquet(f"{export_path}/{name}")


def period(args: list[str], now: datetime) -> tuple[date, date]:
    # Из Airflow без параметров запуска приходят пустые строки
    args = [a for a in args if a]
    if args:
        return date.fromisoformat(args[0]), date.fromisoformat(args[1])
    last = now.date()
    return last - timedelta(days=DEFAULT_DAYS - 1), last


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    now = datetime.now(UTC).replace(tzinfo=None)
    first, last = period(sys.argv[1:], now)
    spark = (
        SparkSession.builder.appName("citybikes_demand")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        # Запас в сутки с обеих сторон покрывает любые смещения часовых поясов
        start = datetime.combine(first - timedelta(days=1), datetime.min.time())
        end = min(datetime.combine(last + timedelta(days=2), datetime.min.time()), now)

        timezones = jdbc(spark, "network_timezones").select("network_id", "timezone")
        history = spark.read.format("delta").load(STATUS_PATH).join(timezones, "network_id", "semi")
        before = (
            history.filter(F.col("observed_at") < F.lit(start))
            .groupBy("network_id", "station_id")
            .agg(F.max_by(F.struct(*history.columns), "observed_at").alias("row"))
            .select("row.*")
        )
        window = history.filter(
            (F.col("observed_at") >= F.lit(start)) & (F.col("observed_at") < F.lit(end))
        )
        hourly = localize(
            station_hours(intervals(before.unionByName(window), start, end)), timezones
        )
        hourly = hourly.cache()

        syncs = jdbc(spark, "network_syncs").filter(
            (F.col("synced_at") >= F.lit(start)) & (F.col("synced_at") < F.lit(end))
        )
        hour_coverage = coverage(syncs, timezones)
        networks = spark.read.format("delta").load(f"{SILVER_PATH}/networks")
        stations = (
            spark.read.format("delta")
            .load(f"{SILVER_PATH}/stations")
            .select(
                "network_id",
                "station_id",
                F.col("name").alias("station_name"),
                F.col("latitude").alias("station_latitude"),
                F.col("longitude").alias("station_longitude"),
            )
        )

        daily = (
            station_daily(hourly, hour_coverage)
            .join(timezones, "network_id")
            .transform(lambda df: finished_days(df, first, last, now))
            .drop("timezone")
            .join(stations, ["network_id", "station_id"], "left")
            .transform(lambda df: with_network_attributes(df, networks))
        )
        weather = jdbc(spark, "weather_hourly").filter(
            (F.col("hour") > F.lit(start)) & (F.col("hour") <= F.lit(end))
        )
        by_hour = (
            network_hourly(hourly, hour_coverage, weather)
            .join(timezones, "network_id")
            .transform(lambda df: finished_days(df, first, last, now))
            .drop("timezone", "hour_utc")
            .transform(lambda df: with_network_attributes(df, networks))
        )

        for name, df in (("station_daily", daily), ("network_hourly", by_hour)):
            write_days(df, name, first, last)
            spark.sql(f"VACUUM delta.`{GOLD_PATH}/{name}` RETAIN {RETENTION_HOURS} HOURS")
            written = spark.read.parquet(f"{EXPORT_PATH}/{name}")
            log.info(
                "%s: %s - %s, %d строк, сетей %d",
                name,
                first,
                last,
                written.count(),
                written.select("network_id").distinct().count(),
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
