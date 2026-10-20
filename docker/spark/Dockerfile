# Официальный образ Spark собран на Ubuntu 22.04 с Python 3.10, а драйвер работает
# в контейнере Airflow на Python 3.12. PySpark требует одинаковую минорную версию Python
# у драйвера и воркеров, берём только JDK и Spark.
FROM apache/spark:4.1.3-scala2.13-java17-ubuntu AS spark-dist

FROM python:3.12-slim-bookworm

COPY --from=spark-dist /opt/java/openjdk /opt/java/openjdk
COPY --from=spark-dist /opt/spark/bin /opt/spark/bin
COPY --from=spark-dist /opt/spark/sbin /opt/spark/sbin
COPY --from=spark-dist /opt/spark/jars /opt/spark/jars
COPY --from=spark-dist /opt/spark/python /opt/spark/python
# JDBC-драйвер Postgres: таблицы источника читают executorы, драйверу Spark он тоже нужен
ADD --checksum=sha256:73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c --chmod=644 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar /opt/spark/jars/
# Delta Lake: те же версии, что в образе Airflow, файлы данных пишут executor'ы
ADD --checksum=sha256:9b05d7a82196ee1f1073733a45a2faae59161e52b86ae918fe6742fad6eaf036 --chmod=644 \
    https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/4.1.0/delta-spark_4.1_2.13-4.1.0.jar /opt/spark/jars/
ADD --checksum=sha256:6d0520ffcc35b582369f92c23fa24e14a5bb935cf6e31ed7f47c5046ae17cb25 --chmod=644 \
    https://repo1.maven.org/maven2/io/delta/delta-storage/4.1.0/delta-storage-4.1.0.jar /opt/spark/jars/

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:$PATH

RUN useradd --uid 185 --create-home spark \
    && mkdir -p /opt/spark/work \
    && chown spark /opt/spark/work

USER spark
WORKDIR /opt/spark
