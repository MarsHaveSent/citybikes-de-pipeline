POSTGRES_CONN_ID = "citybikes_db"

# Лимит API - 300 запросов в час краулер берёт до 150 сетей за прогон
# и останавливается раньше, если в окне осталось меньше резерва под частый опрос станций
CRAWL_BATCH_SIZE = 150
RATE_LIMIT_RESERVE = 120

# Сети для частого опроса: разные климат и континенты, 100-700 станций, свежие данные.
# 20 сетей раз в 10 минут
POLLED_NETWORKS = (
    # Европа
    "mol-bubi",
    "bicing",
    "citybikes-helsinki",
    "oslo-bysykkel",
    "bikemi",
    "gira",
    "le-velo",
    "dublinbikes",
    # Северная Америка
    "bay-wheels",
    "blue-bikes",
    "indego",
    "biketown",
    "mobibikes",
    # Латинская Америка
    "ecobici",
    "bikerio",
    "ecobici-buenos-aires",
    "bikesantiago",
    # Азия, Ближний Восток, Океания
    "docomo-cycle-kyoto",
    "dubai-careem-bike",
    "gobiki",
)
