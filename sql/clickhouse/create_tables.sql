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
    network_operator         LowCardinality(Nullable(String)),
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

-- Колонки, добавленные после создания таблицы: CREATE TABLE IF NOT EXISTS их не добавит
ALTER TABLE stations_obt
    ADD COLUMN IF NOT EXISTS network_operator LowCardinality(Nullable(String))
    AFTER network_companies_count;

-- Витрины спроса (spark_jobs/demand.py): партиция - местные сутки сети,
-- пересчитанные дни подменяются целиком
CREATE TABLE IF NOT EXISTS station_daily
(
    local_date        Date,
    network_id        String,
    station_id        String,
    network_city      LowCardinality(Nullable(String)),
    network_country   LowCardinality(Nullable(String)),
    network_operator  LowCardinality(Nullable(String)),
    station_name      Nullable(String),
    station_latitude  Nullable(Float64),
    station_longitude Nullable(Float64),
    pickups           Int32,
    returns           Int32,
    net_flow          Int32,
    changes           Int32,
    avg_free_bikes    Nullable(Float64),
    avg_ebikes        Nullable(Float64),
    min_free_bikes    Nullable(Int32),
    max_free_bikes    Nullable(Int32),
    empty_hours       Float64,
    -- NULL у сетей, которые не сообщают число свободных мест
    full_hours        Nullable(Float64),
    -- Доля суток, в которую сеть реально опрашивалась (network_syncs)
    observed_share    Float64
)
ENGINE = MergeTree
PARTITION BY local_date
ORDER BY (network_id, station_id);

CREATE TABLE IF NOT EXISTS network_hourly
(
    local_date           Date,
    local_hour           UInt8,
    weekday              UInt8,
    is_weekend           Bool,
    network_id           String,
    network_city         LowCardinality(Nullable(String)),
    network_country      LowCardinality(Nullable(String)),
    network_operator     LowCardinality(Nullable(String)),
    stations             UInt32,
    pickups              Int32,
    returns              Int32,
    bikes_available      Float64,
    ebikes_available     Nullable(Float64),
    empty_station_share  Float64,
    observed_share       Float64,
    temperature          Nullable(Float32),
    apparent_temperature Nullable(Float32),
    precipitation        Nullable(Float32),
    wind_speed           Nullable(Float32),
    weather_code         Nullable(Int16)
)
ENGINE = MergeTree
PARTITION BY local_date
ORDER BY (network_id, local_date, local_hour);
