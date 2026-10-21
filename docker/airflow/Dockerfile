# Версия Spark та же, что в docker/spark: под неё собрана Delta
FROM apache/spark:4.1.1-scala2.13-java17-ubuntu AS spark-dist

FROM apache/airflow:3.3.2-python3.12

# spark-submit запускает драйвер внутри контейнера Airflow, ему нужны JDK и дистрибутив Spark
USER root
COPY --from=spark-dist /opt/java/openjdk /opt/java/openjdk
COPY --from=spark-dist /opt/spark/bin /opt/spark/bin
COPY --from=spark-dist /opt/spark/jars /opt/spark/jars
COPY --from=spark-dist /opt/spark/python /opt/spark/python
ADD --checksum=sha256:73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c --chmod=644 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar /opt/spark/jars/
# Delta Lake для Spark 4.1. Версии 4.2+ тянут свой Hadoop-клиент и Jackson, которые
# конфликтуют с jar-ами Spark, а 4.1.0 обходится этими двумя файлами
ADD --checksum=sha256:9b05d7a82196ee1f1073733a45a2faae59161e52b86ae918fe6742fad6eaf036 --chmod=644 \
    https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/4.1.0/delta-spark_4.1_2.13-4.1.0.jar /opt/spark/jars/
ADD --checksum=sha256:6d0520ffcc35b582369f92c23fa24e14a5bb935cf6e31ed7f47c5046ae17cb25 --chmod=644 \
    https://repo1.maven.org/maven2/io/delta/delta-storage/4.1.0/delta-storage-4.1.0.jar /opt/spark/jars/
USER airflow

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:$PATH

# Версия apache-airflow указана явно, чтобы pip не обновил сам Airflow вместе с провайдером
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "apache-airflow-providers-apache-spark==6.3.2" \
    "clickhouse-connect==1.11.0"
