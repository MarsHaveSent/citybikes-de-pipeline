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
