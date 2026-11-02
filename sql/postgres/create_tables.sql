-- Скрипт выполняется в начале каждого DAG'а, и краулер с опросом стартуют в одну минуту.
-- запуски выстраиваются в очередь; блокировка снимается с концом транзакции
SELECT pg_advisory_xact_lock(hashtext('citybikes_create_tables'));

CREATE TABLE IF NOT EXISTS networks (
    id                 text PRIMARY KEY,
    name               text NOT NULL,
    href               text,
    location           jsonb,
    company            text[],
    gbfs_href          text,
    system             text,
    source             text,
    license            jsonb,
    ebikes             boolean,
    scooters           boolean,
    instances          jsonb,
    first_seen_at      timestamptz NOT NULL,
    last_seen_at       timestamptz NOT NULL,
    stations_synced_at timestamptz
);

-- Текущее состояние станций
CREATE TABLE IF NOT EXISTS stations (
    network_id  text NOT NULL REFERENCES networks (id) ON DELETE CASCADE,
    station_id  text NOT NULL,
    name        text,
    latitude    double precision,
    longitude   double precision,
    -- Строка как в API "...+00:00Z" нормализуется на Sparkе
    "timestamp" text,
    free_bikes  integer,
    empty_slots integer,
    extra       jsonb,
    synced_at   timestamptz NOT NULL,
    PRIMARY KEY (network_id, station_id)
);

-- В extra.ebikes встречаются не только числа, поэтому тип проверяется явно
CREATE OR REPLACE FUNCTION ebikes_count(extra jsonb) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN jsonb_typeof(extra -> 'ebikes') = 'number'
                THEN (extra ->> 'ebikes')::numeric::integer END
$$;

-- Журнал синхронизаций: строка на каждый опрос или обход сети, даже без изменений.
-- Лог ниже пишет только изменения, и по нему не отличить затишье на станциях от простоя
-- опроса;
CREATE TABLE IF NOT EXISTS network_syncs (
    network_id text NOT NULL,
    synced_at  timestamptz NOT NULL,
    stations   integer NOT NULL,
    changes    integer NOT NULL,
    PRIMARY KEY (network_id, synced_at)
);
CREATE INDEX IF NOT EXISTS network_syncs_synced_at_idx ON network_syncs (synced_at);
-- Время, когда CityBikes последний раз обновил данные сети: самый поздний timestamp станций
-- в ответе. Опрос может пройти, а данные - замереть: у сетей Deutsche Bahn это бывает каждый день.
-- NULL - синхронизации до 31.10, когда колонки ещё не было
ALTER TABLE network_syncs ADD COLUMN IF NOT EXISTS feed_updated_at timestamptz;

-- Лог изменений станций: строка появляется, только когда изменилось число велосипедов,
-- свободных слотов или электровелосипедов. Без FK на stations: история переживает удаление станции.
CREATE TABLE IF NOT EXISTS station_status_log (
    network_id  text NOT NULL,
    station_id  text NOT NULL,
    observed_at timestamptz NOT NULL,
    "timestamp" text,
    free_bikes  integer,
    empty_slots integer,
    ebikes      integer,
    PRIMARY KEY (network_id, station_id, observed_at)
);
CREATE INDEX IF NOT EXISTS station_status_log_observed_at_idx
    ON station_status_log (observed_at);

-- Реестр полей из ответов API для отслеживания дрейфа схемы
CREATE TABLE IF NOT EXISTS schema_fields (
    entity        text NOT NULL,
    field         text NOT NULL,
    first_seen_at timestamptz NOT NULL,
    last_seen_at  timestamptz NOT NULL,
    PRIMARY KEY (entity, field)
);

-- Часовой пояс сети: Open-Meteo определяет его по координатам вместе с погодой.
CREATE TABLE IF NOT EXISTS network_timezones (
    network_id text PRIMARY KEY,
    timezone   text NOT NULL,
    updated_at timestamptz NOT NULL
);

-- Почасовая погода в городах опрашиваемых сетей (Open-Meteo).
-- Осадки - сумма за час, который заканчивается в hour, остальное - значение на момент hour
CREATE TABLE IF NOT EXISTS weather_hourly (
    network_id           text NOT NULL,
    hour                 timestamptz NOT NULL,
    temperature          real,
    apparent_temperature real,
    precipitation        real,
    wind_speed           real,
    weather_code         smallint,
    fetched_at           timestamptz NOT NULL,
    PRIMARY KEY (network_id, hour)
);

-- Журнал задач пайплайна: одна строка на попытку задачи, пишут колбэки DAG'ов
-- (citybikes.monitoring). metrics - то, что вернула задача: синхронизировано сетей, строк и т.п.
CREATE TABLE IF NOT EXISTS ops_task_runs (
    dag_id      text NOT NULL,
    task_id     text NOT NULL,
    run_id      text NOT NULL,
    try_number  integer NOT NULL,
    state       text NOT NULL,
    started_at  timestamptz,
    finished_at timestamptz NOT NULL,
    duration_s  real,
    metrics     jsonb,
    error       text,
    PRIMARY KEY (dag_id, task_id, run_id, try_number)
);
CREATE INDEX IF NOT EXISTS ops_task_runs_finished_at_idx ON ops_task_runs (finished_at);

-- Результаты проверок качества данных (citybikes.dq)
CREATE TABLE IF NOT EXISTS ops_dq_results (
    checked_at timestamptz NOT NULL,
    layer      text NOT NULL,
    check_name text NOT NULL,
    value      double precision,
    warn_at    double precision,
    fail_at    double precision,
    status     text NOT NULL,
    run_id     text,
    PRIMARY KEY (check_name, checked_at)
);

-- Справочники GeoNames (CC BY 4.0), обновляются раз в месяц целиком
CREATE TABLE IF NOT EXISTS geo_countries (
    iso2       text PRIMARY KEY,
    name       text NOT NULL,
    continent  text,
    population bigint NOT NULL,
    loaded_at  timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS geo_cities (
    geoname_id bigint PRIMARY KEY,
    name       text NOT NULL,
    country    text NOT NULL,
    latitude   double precision NOT NULL,
    longitude  double precision NOT NULL,
    population bigint NOT NULL,
    timezone   text,
    loaded_at  timestamptz NOT NULL
);

-- Представления для дашборда здоровья пайплайна в Yandex DataLens. Пользователь datalens видит
-- только их: без текста ошибок, паролей подключений и сырых таблиц
CREATE OR REPLACE VIEW health_task_runs AS
SELECT
    dag_id,
    task_id,
    run_id,
    try_number,
    state,
    started_at,
    finished_at,
    duration_s,
    run_id LIKE 'manual__%' AS is_manual,
    -- Только класс исключения: в тексте бывают команды spark-submit и пути
    substring(error FROM '^[A-Za-z_.]+') AS error_type
FROM ops_task_runs;

CREATE OR REPLACE VIEW health_dq_results AS
SELECT checked_at, layer, check_name, value, warn_at, fail_at, status, run_id
FROM ops_dq_results;

-- Сеть × местные сутки: полнота наблюдения по тому же правилу, что в spark_jobs/demand.py
-- (10-минутный слот засчитывается, если данные свежее 30 минут), и «живость» - доля станций,
-- изменившихся за опрос. Сети, которые только обходит краулер (4-6 раз в сутки), не попадают
CREATE OR REPLACE VIEW health_network_days AS
WITH syncs AS (
    SELECT
        s.network_id,
        s.synced_at,
        s.stations,
        s.changes,
        tz.timezone,
        (s.synced_at AT TIME ZONE tz.timezone)::date AS local_date,
        s.feed_updated_at IS NULL
            OR s.synced_at - s.feed_updated_at <= interval '30 minutes' AS fresh
    FROM network_syncs s
    JOIN network_timezones tz USING (network_id)
)
SELECT
    syncs.network_id,
    n.name AS network_name,
    n.location ->> 'city' AS city,
    syncs.local_date,
    count(*) AS syncs,
    count(*) FILTER (WHERE NOT fresh) AS stale_syncs,
    -- Длина суток - между местными полуночами: в дни перевода часов 23 или 25 часов
    least(
        count(DISTINCT floor(extract(epoch FROM syncs.synced_at) / 600)) FILTER (WHERE fresh)
            / (extract(epoch FROM ((syncs.local_date + 1)::timestamp AT TIME ZONE syncs.timezone)
                - (syncs.local_date::timestamp AT TIME ZONE syncs.timezone)) / 600),
        1
    ) AS observed_share,
    sum(syncs.changes) AS changes,
    avg(syncs.changes::numeric / nullif(syncs.stations, 0)) AS changed_share
FROM syncs
JOIN networks n ON n.id = syncs.network_id
GROUP BY syncs.network_id, n.name, n.location ->> 'city', syncs.local_date, syncs.timezone
HAVING count(*) >= 24;

-- Объёмы за сутки UTC из метрик, которые задачи возвращают в ops_task_runs
CREATE OR REPLACE VIEW health_daily_volumes AS
SELECT
    finished_at::date AS day,
    sum((metrics ->> 'synced')::bigint) FILTER (WHERE task_id = 'poll') AS poll_syncs,
    sum((metrics ->> 'synced')::bigint) FILTER (WHERE task_id = 'crawl') AS crawl_syncs,
    sum((metrics ->> 'changes')::bigint) FILTER (WHERE task_id IN ('poll', 'crawl')) AS log_rows,
    sum((metrics ->> 'hours')::bigint) FILTER (WHERE task_id = 'load_weather') AS weather_hours,
    sum((metrics ->> 'station_daily')::bigint + (metrics ->> 'network_hourly')::bigint)
        FILTER (WHERE task_id = 'load_demand') AS demand_rows_loaded,
    sum(coalesce((metrics ->> 'stations_obt')::bigint, (metrics ->> 'rows')::bigint))
        FILTER (WHERE task_id = 'load_clickhouse') AS obt_rows_loaded
FROM ops_task_runs
WHERE state = 'success' AND metrics IS NOT NULL
GROUP BY finished_at::date;

DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'datalens') THEN
        GRANT SELECT ON health_task_runs, health_dq_results, health_network_days,
            health_daily_volumes TO datalens;
    END IF;
END
$$;
