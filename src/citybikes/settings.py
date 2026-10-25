POSTGRES_CONN_ID = "citybikes_db"
CLICKHOUSE_CONN_ID = "citybikes_clickhouse"

# Выгрузки витрин для ClickHouse, по каталогу на таблицу (spark_jobs/*.py, EXPORT_PATH)
EXPORT_URL = "hdfs://namenode:8020/citybikes/export"

# Лимит API - 300 запросов в час краулер берёт до 150 сетей за прогон
# и останавливается раньше, если в окне осталось меньше резерва под частый опрос станций
CRAWL_BATCH_SIZE = 150
RATE_LIMIT_RESERVE = 120

# Сети для частого опроса - Германия: один часовой пояс и сравнимые города, а в Берлине,
# Мюнхене и Кёльне работают по два оператора. 20 сетей раз в 10 минут
POLLED_NETWORKS = (
    # nextbike и Call a Bike (Deutsche Bahn)
    "nextbike-berlin",
    "callabike-berlin",
    "nextbike-myradl",
    "callabike-munchen",
    "kvb-rad-koln",
    "callabike-koln",
    # Deutsche Bahn
    "stadtrad-hamburg-db",
    "callabike-frankfurt",
    "stadtrad-stuttgart",
    # nextbike
    "mobibike-dresden",
    "nextbike-dusseldorf",
    "nextbike-leipzig",
    "vrn-nextbike",
    "nextbike-bonn",
    "mvg-meinrad-nextbike-mainz",
    "sprintrad",
    "wk-bike-bremen-bremen",
    "nibelungen-bike-braunschweig",
    "frelo-freiburg",
    # Donkey Republic - третий оператор
    "sprottenflotte-kiel",
)
