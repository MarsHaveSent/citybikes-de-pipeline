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
