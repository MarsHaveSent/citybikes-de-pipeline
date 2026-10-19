from pyspark.sql import functions as F

from spark_jobs.silver import transform_networks, transform_stations
from spark_jobs.stations_obt import build_obt, write_snapshot

OBT_COLUMNS = [
    "snapshot_date",
    "network_id",
    "network_name",
    "network_latitude",
    "network_longitude",
    "network_city",
    "network_country",
    "network_companies",
    "network_companies_count",
    "network_system",
    "station_id",
    "station_name",
    "network_station",
    "station_latitude",
    "station_longitude",
    "station_timestamp",
    "station_free_bikes",
    "station_empty_slots",
    "station_renting",
    "station_returning",
    "station_last_updated",
    "station_slots",
    "station_ebikes",
    "station_has_ebikes",
    "station_payment",
    "station_payment_terminal",
    "station_address",
    "station_post_code",
    "station_synced_at",
]


def test_obt_keeps_networks_without_stations(networks_df, stations_df):
    networks = transform_networks(
        networks_df({"id": "bubi", "name": "Bubi"}, {"id": "empty", "name": "Empty"})
    )
    stations = transform_stations(
        stations_df(
            {"network_id": "bubi", "station_id": "1", "name": "Deák", "free_bikes": 3},
            {"network_id": "bubi", "station_id": "2", "name": "Oktogon", "free_bikes": 0},
        )
    )

    obt = build_obt(networks, stations, "2026-10-18")
    rows = sorted(obt.collect(), key=lambda r: (r.network_id, r.station_id or ""))

    assert obt.columns == OBT_COLUMNS
    assert {str(r.snapshot_date) for r in rows} == {"2026-10-18"}
    assert [(r.network_id, r.station_id, r.network_station) for r in rows] == [
        ("bubi", "1", "Bubi/Deák"),
        ("bubi", "2", "Bubi/Oktogon"),
        ("empty", None, None),
    ]


def test_rewriting_snapshot_keeps_other_days(spark, networks_df, stations_df, tmp_path):
    networks = transform_networks(networks_df({"id": "bubi", "name": "Bubi"}))
    stations = transform_stations(stations_df())
    path = str(tmp_path / "stations_obt")

    write_snapshot(build_obt(networks, stations, "2026-10-17"), path)
    write_snapshot(build_obt(networks, stations, "2026-10-18"), path)
    renamed = networks.withColumn("name", F.lit("Bubi renamed"))
    write_snapshot(build_obt(renamed, stations, "2026-10-18"), path)

    rows = spark.read.parquet(path).orderBy("snapshot_date").collect()
    assert [(str(r.snapshot_date), r.network_name) for r in rows] == [
        ("2026-10-17", "Bubi"),
        ("2026-10-18", "Bubi renamed"),
    ]
