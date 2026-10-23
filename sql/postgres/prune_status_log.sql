-- Сырой лог изменений хранится в Postgres 30 дней. Задача идёт после загрузки в silver
-- (citybikes_demand), поэтому удаляемые строки там уже есть, и история остаётся целиком
DELETE FROM station_status_log WHERE observed_at < now() - interval '30 days';
