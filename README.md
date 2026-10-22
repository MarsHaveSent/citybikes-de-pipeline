# CityBikes DE Pipeline

[![CI](https://github.com/MarsHaveSent/citybikes-de-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/MarsHaveSent/citybikes-de-pipeline/actions/workflows/ci.yml)

ELT-пайплайн на данных сетей велошеринга: загрузка из CityBikes API в PostgreSQL, обработка на Spark, хранение витрин в HDFS (Delta Lake), аналитика в ClickHouse и Metabase. Весь стек поднимается в Docker.

Пайплайн регулярно собирает каталог сетей и состояние станций с учётом лимита API, обогащает данные погодой и ищет крупные города без велошеринга.

> Проект в разработке.

## Стек

Python 3.12, Apache Airflow 3, PostgreSQL, Apache Spark, HDFS, Delta Lake, ClickHouse, Metabase, Docker, uv, GitHub Actions.

## Слои данных

| Слой | Где | Что |
|---|---|---|
| bronze | PostgreSQL | Сети и станции в том виде, в каком их отдаёт API, лог изменений станций, погода |
| silver | HDFS `/citybikes/silver`, Delta | Очищенные сети и станции: JSON-поля разобраны, типы приведены, время в UTC |
| gold | HDFS `/citybikes/gold`, Delta → ClickHouse | Витрины: `stations_obt` — плоская таблица станций с атрибутами сетей, срез инфраструктуры. Раз в сутки пишется снимок в партицию `snapshot_date`: внутри дня данные не меняются, сравнение снимков показывает, как растёт сеть станций |

Silver и gold — таблицы Delta Lake. Запись атомарна: если джоба упадёт посередине, читатели видят предыдущую версию таблицы, а не пустую или частично записанную папку. Silver пересобирается целиком, снимок в gold заменяется через `replaceWhere` только за свою дату, поэтому перезапуск не задевает другие дни. `VACUUM` после записи хранит версии за неделю: на них работает time travel (`VERSION AS OF`), более старые файлы удаляются. Delta 4.1.0 выбран под Spark 4.1: более новые версии тянут собственные Hadoop-клиент и Jackson, которые конфликтуют с библиотеками Spark. Spark зафиксирован на 4.1.1: Delta 4.1.0 собрана под Spark 4.1.0, а начиная с 4.1.2 в Spark изменены внутренние классы, и часть команд Delta (`CONVERT TO DELTA`, API `DeltaTable`) падает. В 4.1.1 эти классы те же, что в 4.1.0.

Снимок витрины попадает в ClickHouse так: Spark выгружает день отдельным parquet-файлом в `/citybikes/export/stations_obt`, ClickHouse читает его сам табличной функцией `hdfs()` во временную таблицу, сверяет дату и число строк и подменяет день в `citybikes.stations_obt` одной командой `REPLACE PARTITION`. Подмена атомарна: запросы видят либо прежний снимок дня, либо новый. Прямо из Delta-таблицы ClickHouse читать не может: его движок Delta Lake не поддерживает HDFS, а parquet-файлы самой таблицы содержат и заменённые версии снимков. Spark JDBC отпал из-за колонок-массивов: у Spark нет диалекта ClickHouse, и массивы по JDBC не пишутся.

Spark читает bronze напрямую из PostgreSQL по JDBC. В продакшене для этого берут реплику или поток изменений (CDC), чтобы аналитические запросы не нагружали рабочую базу; здесь источник служит только пайплайну.

Счётчики велосипедов в `stations_obt` — состояние на момент снимка (01:00 UTC), а это ночь в Европе и вечер в Америке, поэтому для анализа спроса они не подходят. Спрос считается по логу изменений станций в местных сутках каждой сети. Подробная история есть у 20 сетей, которые опрашиваются раз в 10 минут; остальные сети краулер обходит раз в 5–6 часов, их данные используются для анализа инфраструктуры.

В данных `last_updated` встречается пять форматов времени, в том числе без часового пояса. Значения без пояса считаются UTC.

## Мониторинг и качество данных

- **Журнал задач `ops_task_runs`** (PostgreSQL). Колбэки, общие для всех DAG'ов, пишут каждую попытку задачи: статус, длительность, текст ошибки и метрики, которые задача вернула (сколько сетей синхронизировано, упёрся ли краулер в лимит API, сколько строк загружено в ClickHouse). Метаданные самого Airflow для этого не используются: их схема меняется между версиями, и бизнес-метрик в них нет.
- **Проверки качества `ops_dq_results`.** Проверка — SQL-запрос, возвращающий одно число, и пороги `warn`/`fail` ([src/citybikes/dq.py](src/citybikes/dq.py)). Bronze проверяется раз в час (DAG `citybikes_dq`): отставание опрашиваемых сетей, покрытие краулера за сутки, свежесть каталога и погоды, новые поля в ответах API. Gold — после каждой загрузки снимка в ClickHouse: снимок не пуст, число строк и сетей не прыгнуло относительно прошлого дня, нет дублей станций, доли пропусков в норме. Great Expectations и Soda не взяты: для двух десятков проверок они тяжелее по памяти и сложнее, чем сами проверки.
- **Алерты на почту.** Окончательно упавшая задача (после ретраев) и непройденная `fail`-проверка отправляют письмо через SMTP. Одно письмо приходит, когда задача сломалась, и одно — когда снова заработала; пока она продолжает падать, напоминание не чаще раза в сутки. Запись в журнал и отправка письма независимы.
- Журналы мониторинга хранятся 90 дней, старые записи удаляет `citybikes_dq`.

На данных в несколько мегабайт настройки Spark по умолчанию оказались главным тормозом: VACUUM в Delta собирал список файлов в 10 000 параллельных задач, а внутренние операции Delta резали shuffle на 200 частей — 23 тыс. задач на один запуск silver. После уменьшения параллелизма ([docker/spark/spark-defaults.conf](docker/spark/spark-defaults.conf)) задач стало около тысячи, а silver и витрина считаются в 2.5–3 раза быстрее.

## Запуск локально

Нужны Docker и Python 3.

```bash
python scripts/init_env.py   # создаёт .env со случайными паролями; в существующий дописывает новые ключи
docker compose up -d
```

- Airflow: http://localhost:8080, логин и пароль — `AIRFLOW_ADMIN_USER` и `AIRFLOW_ADMIN_PASSWORD` из `.env`
- PostgreSQL: `localhost:15432`, БД `citybikes`, пользователь `citybikes`
- Spark Master UI: http://localhost:8081, Spark Worker UI: http://localhost:8082
- HDFS NameNode UI: http://localhost:9870
- ClickHouse: http://localhost:8123, БД `citybikes`, пользователь `citybikes`, пароль `CLICKHOUSE_PASSWORD` из `.env`

## Тесты

```bash
uv sync
uv run pytest
```

Тесты из `tests/integration` работают с PostgreSQL и ClickHouse и без переменных `TEST_DATABASE_URL` и `TEST_CLICKHOUSE_URL` пропускаются. Каждый тест создаёт себе отдельную схему (базу в ClickHouse) и удаляет её после себя, поэтому подойдут любые серверы, например временные контейнеры:

```bash
docker run -d --rm --name pg-test -e POSTGRES_PASSWORD=test -p 127.0.0.1:15499:5432 postgres:17.11-trixie
docker run -d --rm --name ch-test -e CLICKHOUSE_USER=test -e CLICKHOUSE_PASSWORD=test -p 127.0.0.1:18123:8123 clickhouse/clickhouse-server:26.8.32.14
TEST_DATABASE_URL=postgresql://postgres:test@127.0.0.1:15499/postgres \
  TEST_CLICKHOUSE_URL=http://test:test@127.0.0.1:18123 uv run pytest
```

В CI на каждый пуш запускаются pre-commit, все тесты с PostgreSQL и ClickHouse и проверка DAGов в образе Airflow (`scripts/check_dags.py`).

## Деплой

Сервер обновляется при пуше в `main`, если прошли все проверки CI:

1. Образы Airflow, Spark и Hadoop собираются в GitHub Actions и публикуются в GHCR. Тег образа — хеш его каталога `docker/<образ>` в git ([deploy/image-tag.sh](deploy/image-tag.sh)), поэтому образ пересобирается, только когда меняются его файлы.
2. По SSH на сервере запускается [deploy/deploy.sh](deploy/deploy.sh): код переключается на этот коммит, образы — на теги, вычисленные из него же, стек перезапускается, скрипт ждёт, пока сервисы пройдут healthcheck. DAG'и, код и SQL смонтированы в контейнеры и обновляются вместе с git, без пересборки образов.

На сервере к `docker-compose.yml` подключается [deploy/compose.prod.yml](deploy/compose.prod.yml): образы из GHCR вместо локальной сборки и Caddy, который получает сертификат Let's Encrypt и открывает Airflow по адресу `https://<домен>/airflow/`. Остальные сервисы наружу не публикуются. Первичная настройка чистого сервера — [deploy/bootstrap.sh](deploy/bootstrap.sh).

## Источник данных

Данные о сетях и станциях предоставляет [CityBikes](https://citybik.es/) через [API v2](https://api.citybik.es/v2/).

Погодные данные: [Open-Meteo](https://open-meteo.com/), лицензия [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
