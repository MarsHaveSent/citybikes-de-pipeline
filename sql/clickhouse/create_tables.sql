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

-- Мировой рынок (spark_jobs/market.py): снимок на дату, как stations_obt
CREATE TABLE IF NOT EXISTS market_by_country
(
    snapshot_date        Date,
    country              String,
    country_name         Nullable(String),
    continent            LowCardinality(Nullable(String)),
    population           Nullable(Int64),
    networks             UInt32,
    stations             UInt32,
    operators            UInt32,
    -- Индекс Херфиндаля-Хиршмана по долям станций операторов: от ~0 до 10 000
    hhi                  Nullable(Int32),
    top_operator         Nullable(String),
    top_operator_share   Nullable(Float64),
    ebike_station_share  Nullable(Float64),
    stations_per_million Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY snapshot_date
ORDER BY country;

CREATE TABLE IF NOT EXISTS operator_presence
(
    snapshot_date Date,
    operator      String,
    country       String,
    networks      UInt32,
    stations      UInt32
)
ENGINE = MergeTree
PARTITION BY snapshot_date
ORDER BY (operator, country);

CREATE TABLE IF NOT EXISTS growth_points
(
    snapshot_date      Date,
    geoname_id         UInt64,
    city               String,
    country            String,
    latitude           Float64,
    longitude          Float64,
    population         UInt64,
    nearest_network_id Nullable(String),
    nearest_network_km Nullable(Float64),
    stations_in_radius UInt32,
    stations_per_100k  Float64,
    without_network    Bool
)
ENGINE = MergeTree
PARTITION BY snapshot_date
ORDER BY (country, geoname_id);

-- Представления для Metabase: правила отбора, общие для всех карточек дашбордов,
-- задаются здесь один раз. Снимки - последняя загруженная дата
CREATE OR REPLACE VIEW bi_stations AS
SELECT * FROM stations_obt
WHERE snapshot_date = (SELECT max(snapshot_date) FROM stations_obt);

CREATE OR REPLACE VIEW bi_market_by_country AS
SELECT * FROM market_by_country
WHERE snapshot_date = (SELECT max(snapshot_date) FROM market_by_country);

CREATE OR REPLACE VIEW bi_operator_presence AS
SELECT * FROM operator_presence
WHERE snapshot_date = (SELECT max(snapshot_date) FROM operator_presence);

CREATE OR REPLACE VIEW bi_growth_points AS
SELECT * FROM growth_points
WHERE snapshot_date = (SELECT max(snapshot_date) FROM growth_points);

-- Спрос: только сутки сети, которые опрос покрыл хотя бы на 90%.
-- weekday - с понедельника (1) по воскресенье (7). В network_hourly он в нумерации Spark,
-- с воскресенья, поэтому в представлении пересчитан
CREATE OR REPLACE VIEW bi_station_daily AS
SELECT
    *,
    toDayOfWeek(local_date) AS weekday,
    concat(toString(weekday), ' ', ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'][weekday]) AS weekday_name,
    if(weekday >= 6, 'выходные', 'будни') AS day_type
FROM station_daily
WHERE observed_share >= 0.9;

-- Полнота берётся из station_daily: в network_hourly нет строк за часы без данных
CREATE OR REPLACE VIEW bi_network_hourly AS
SELECT
    * REPLACE (toDayOfWeek(local_date) AS weekday),
    concat(
        toString(toDayOfWeek(local_date)), ' ',
        ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'][toDayOfWeek(local_date)]
    ) AS weekday_name,
    if(toDayOfWeek(local_date) >= 6, 'выходные', 'будни') AS day_type,
    CAST(precipitation > 0.5 AS Nullable(Bool)) AS is_rainy
FROM network_hourly
WHERE (network_id, local_date) IN (
    SELECT network_id, local_date FROM station_daily WHERE observed_share >= 0.9
);
