-- Снимки витрины станций (gold stations_obt). Каждый день - отдельная партиция,
-- загрузка подменяет её целиком
CREATE TABLE IF NOT EXISTS stations_obt
(
    snapshot_date            Date,
    network_id               String,
    network_name             String,
    network_latitude         Nullable(Float64),
    network_longitude        Nullable(Float64),
    network_city             LowCardinality(Nullable(String)),
    network_country          LowCardinality(Nullable(String)),
    network_companies        Array(String),
    network_companies_count  UInt16,
    network_system           LowCardinality(Nullable(String)),
    -- У сетей без станций пустая строка: колонки ключа сортировки не могут быть Nullable
    station_id               String,
    station_name             Nullable(String),
    network_station          Nullable(String),
    station_latitude         Nullable(Float64),
    station_longitude        Nullable(Float64),
    station_timestamp        Nullable(DateTime('UTC')),
    station_free_bikes       Nullable(Int32),
    station_empty_slots      Nullable(Int32),
    station_renting          Nullable(Bool),
    station_returning        Nullable(Bool),
    station_last_updated     Nullable(DateTime('UTC')),
    station_slots            Nullable(Int32),
    station_ebikes           Nullable(Int32),
    station_has_ebikes       Nullable(Bool),
    station_payment          Array(String),
    station_payment_terminal Nullable(Bool),
    station_address          Nullable(String),
    station_post_code        Nullable(String),
    station_synced_at        Nullable(DateTime('UTC'))
)
ENGINE = MergeTree
PARTITION BY snapshot_date
ORDER BY (network_id, station_id);
