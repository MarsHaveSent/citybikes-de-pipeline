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

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:$PATH

RUN useradd --uid 185 --create-home spark \
    && mkdir -p /opt/spark/work \
    && chown spark /opt/spark/work

USER spark
WORKDIR /opt/spark
