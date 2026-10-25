"""Gold-слой: мировой рынок велошеринга по снимку stations_obt.

- market_by_country: страна - станции, сети, операторы, станций на 1 млн жителей,
  концентрация рынка (индекс Херфиндаля-Хиршмана), крупнейший оператор, доля станций с электро
- operator_presence: оператор × страна - сети и станции
- growth_points: города от 300 тыс. жителей - до ближайшей сети, станций в радиусе города

Население и города - справочники GeoNames из Postgres.
    spark-submit market.py 2026-10-25
"""

import logging
import os
import sys

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger("citybikes.market")

OBT_PATH = "/citybikes/gold/stations_obt"
GOLD_PATH = "/citybikes/gold"
EXPORT_PATH = "/citybikes/export"
RETENTION_HOURS = 168

BIG_CITY_POPULATION = 300_000
# Дальше этого от центра ближайшей сети город считается без велошеринга
NO_NETWORK_KM = 25
# Радиус, в котором станции относятся к городу при подсчёте покрытия
CITY_RADIUS_KM = 10
# Ячейка сетки для отбора станций рядом с городом: 0.5° долготы - не меньше 19 км до 70°
# широты, поэтому соседние ячейки накрывают радиус города
GRID_DEGREES = 0.5


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    a = F.pow(F.sin(F.radians(lat2 - lat1) / 2), 2) + F.cos(F.radians(lat1)) * F.cos(
        F.radians(lat2)
    ) * F.pow(F.sin(F.radians(lon2 - lon1) / 2), 2)
    return 2 * 6371.0 * F.asin(F.sqrt(a))


def networks_of(obt: DataFrame) -> DataFrame:
    """Сети снимка; без оператора - сеть сама себе оператор, иначе она пропала бы из долей."""
    return obt.groupBy("network_id").agg(
        F.first("network_country").alias("country"),
        F.coalesce(F.first("network_operator"), F.first("network_name")).alias("operator"),
        F.first("network_latitude").alias("latitude"),
        F.first("network_longitude").alias("longitude"),
        F.count("station_id").alias("stations"),
        F.count(F.when(F.col("station_has_ebikes") | (F.col("station_ebikes") > 0), 1)).alias(
            "ebike_stations"
        ),
        # Сообщают ли станции сети что-то про электро
        F.count(
            F.when(F.col("station_has_ebikes").isNotNull() | F.col("station_ebikes").isNotNull(), 1)
        ).alias("ebike_reported_stations"),
    )


def operator_presence(networks: DataFrame) -> DataFrame:
    return networks.groupBy("operator", "country").agg(
        F.count("network_id").alias("networks"),
        F.sum("stations").alias("stations"),
    )


def market_by_country(presence: DataFrame, networks: DataFrame, countries: DataFrame) -> DataFrame:
    country = Window.partitionBy("country")
    shares = presence.withColumn(
        # try_divide: у стран, где ни одна сеть не отдаёт станции, доля не определена (NULL);
        # обычное деление на ноль в ANSI-режиме Spark 4 роняет джобу
        "share",
        F.try_divide(F.col("stations"), F.sum("stations").over(country)),
    ).withColumn("rank", F.row_number().over(country.orderBy(F.desc("stations"), "operator")))
    concentration = shares.groupBy("country").agg(
        F.round(F.sum(F.pow(F.col("share") * 100, 2))).cast("int").alias("hhi"),
        F.max(F.when(F.col("rank") == 1, F.col("operator"))).alias("top_operator"),
        F.max(F.when(F.col("rank") == 1, F.col("share"))).alias("top_operator_share"),
        F.count("operator").alias("operators"),
    )
    totals = networks.groupBy("country").agg(
        F.count("network_id").alias("networks"),
        F.sum("stations").alias("stations"),
        F.try_divide(F.sum("ebike_stations"), F.sum("ebike_reported_stations")).alias(
            "ebike_station_share"
        ),
    )
    return (
        totals.join(concentration, "country")
        .join(
            countries.select(
                F.col("iso2").alias("country"),
                F.col("name").alias("country_name"),
                "continent",
                "population",
            ),
            "country",
            "left",
        )
        .withColumn(
            "stations_per_million",
            F.when(F.col("population") > 0, F.col("stations") / F.col("population") * 1e6),
        )
    )


def growth_points(cities: DataFrame, networks: DataFrame, stations: DataFrame) -> DataFrame:
    big = cities.filter(F.col("population") >= BIG_CITY_POPULATION)
    located = networks.filter(F.col("latitude").isNotNull()).select(
        F.col("network_id").alias("nearest_network_id"),
        F.col("latitude").alias("n_lat"),
        F.col("longitude").alias("n_lon"),
    )
    distance = haversine_km(F.col("latitude"), F.col("longitude"), F.col("n_lat"), F.col("n_lon"))
    nearest = (
        big.crossJoin(F.broadcast(located))
        .withColumn("km", distance)
        .groupBy("geoname_id")
        .agg(
            F.min_by("nearest_network_id", "km").alias("nearest_network_id"),
            F.min("km").alias("km"),
        )
    )

    def cell(lat: Column, lon: Column) -> Column:
        return F.struct(
            F.floor(lat / GRID_DEGREES).alias("y"), F.floor(lon / GRID_DEGREES).alias("x")
        )

    offsets = F.array(
        *[
            F.struct(F.lit(dy).alias("dy"), F.lit(dx).alias("dx"))
            for dy in (-1, 0, 1)
            for dx in (-1, 0, 1)
        ]
    )
    city_cells = big.select(
        "geoname_id", "latitude", "longitude", F.explode(offsets).alias("o")
    ).select(
        "geoname_id",
        F.col("latitude").alias("c_lat"),
        F.col("longitude").alias("c_lon"),
        F.struct(
            (F.floor(F.col("latitude") / GRID_DEGREES) + F.col("o.dy")).alias("y"),
            (F.floor(F.col("longitude") / GRID_DEGREES) + F.col("o.dx")).alias("x"),
        ).alias("cell"),
    )
    station_cells = stations.select(
        F.col("station_latitude").alias("s_lat"),
        F.col("station_longitude").alias("s_lon"),
        cell(F.col("station_latitude"), F.col("station_longitude")).alias("cell"),
    )
    covered = (
        city_cells.join(station_cells, "cell")
        .filter(
            haversine_km(F.col("c_lat"), F.col("c_lon"), F.col("s_lat"), F.col("s_lon"))
            <= CITY_RADIUS_KM
        )
        .groupBy("geoname_id")
        .agg(F.count("*").alias("stations_in_radius"))
    )
    return (
        big.join(nearest, "geoname_id", "left")
        .join(covered, "geoname_id", "left")
        .select(
            "geoname_id",
            F.col("name").alias("city"),
            "country",
            "latitude",
            "longitude",
            "population",
            "nearest_network_id",
            F.round("km", 1).alias("nearest_network_km"),
            F.coalesce("stations_in_radius", F.lit(0)).alias("stations_in_radius"),
            (F.coalesce("stations_in_radius", F.lit(0)) / F.col("population") * 1e5).alias(
                "stations_per_100k"
            ),
            (F.col("km").isNull() | (F.col("km") > NO_NETWORK_KM)).alias("without_network"),
        )
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


def write_snapshot(
    df: DataFrame,
    name: str,
    snapshot_date: str,
    gold_path: str = GOLD_PATH,
    export_path: str = EXPORT_PATH,
) -> None:
    out = df.withColumn("snapshot_date", F.lit(snapshot_date).cast("date")).coalesce(1)
    (
        out.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_date = DATE'{snapshot_date}'")
        .option("mergeSchema", "true")
        .partitionBy("snapshot_date")
        .save(f"{gold_path}/{name}")
    )
    out.write.mode("overwrite").parquet(f"{export_path}/{name}")


def main() -> None:
    snapshot_date = sys.argv[1]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = (
        SparkSession.builder.appName("citybikes_market")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        obt = (
            spark.read.format("delta")
            .load(OBT_PATH)
            .filter(F.col("snapshot_date") == snapshot_date)
        )
        networks = networks_of(obt).cache()
        presence = operator_presence(networks)
        marts = {
            "market_by_country": market_by_country(
                presence, networks, jdbc(spark, "geo_countries")
            ),
            "operator_presence": presence,
            "growth_points": growth_points(
                jdbc(spark, "geo_cities"), networks, obt.filter(F.col("station_id").isNotNull())
            ),
        }
        for name, df in marts.items():
            write_snapshot(df, name, snapshot_date)
            spark.sql(f"VACUUM delta.`{GOLD_PATH}/{name}` RETAIN {RETENTION_HOURS} HOURS")
            log.info(
                "%s, снимок %s: %d строк",
                name,
                snapshot_date,
                spark.read.parquet(f"{EXPORT_PATH}/{name}").count(),
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
