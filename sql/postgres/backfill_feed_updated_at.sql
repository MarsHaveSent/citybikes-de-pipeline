-- Разовое восстановление network_syncs.feed_updated_at для синхронизаций до 31.10.
-- Время обновления берётся из лога: самый поздний timestamp станций сети, увиденный к моменту
-- синхронизации.
WITH seen AS (
    SELECT network_id, observed_at, max(rtrim("timestamp", 'Z')::timestamptz) AS feed
    FROM station_status_log
    WHERE observed_at >= '2026-10-15' AND "timestamp" LIKE '%+00:00Z'
    GROUP BY network_id, observed_at
),
latest AS (
    SELECT ns.network_id, ns.synced_at,
           max(seen.feed) OVER (PARTITION BY ns.network_id ORDER BY ns.synced_at) AS feed
    FROM network_syncs ns
    LEFT JOIN seen ON seen.network_id = ns.network_id AND seen.observed_at = ns.synced_at
    WHERE ns.synced_at >= '2026-10-15' AND ns.synced_at < '2026-11-01'
),
gaps AS (
    SELECT network_id, synced_at, feed,
           max(synced_at - feed) OVER (PARTITION BY network_id, feed) AS gap
    FROM latest
)
UPDATE network_syncs ns
SET feed_updated_at = gaps.feed
FROM gaps
WHERE ns.network_id = gaps.network_id
  AND ns.synced_at = gaps.synced_at
  AND ns.feed_updated_at IS NULL
  AND gaps.gap > interval '6 hours'
  -- Перерывы из-за редкого обхода сетей вне частого опроса заморозкой не считаются
  AND ns.network_id IN (
      'callabike-berlin', 'callabike-munchen', 'callabike-koln',
      'stadtrad-hamburg-db', 'callabike-frankfurt', 'stadtrad-stuttgart'
  );
