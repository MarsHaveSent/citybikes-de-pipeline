FROM apache/spark:4.1.3-scala2.13-java17-ubuntu AS spark-dist

FROM apache/airflow:3.3.2-python3.12

# spark-submit запускает драйвер внутри контейнера Airflow, ему нужны JDK и дистрибутив Spark
USER root
COPY --from=spark-dist /opt/java/openjdk /opt/java/openjdk
COPY --from=spark-dist /opt/spark/bin /opt/spark/bin
COPY --from=spark-dist /opt/spark/jars /opt/spark/jars
COPY --from=spark-dist /opt/spark/python /opt/spark/python
ADD --checksum=sha256:73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c --chmod=644 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar /opt/spark/jars/
USER airflow

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:$PATH

# Версия apache-airflow указана явно, чтобы pip не обновил сам Airflow вместе с провайдером
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "apache-airflow-providers-apache-spark==6.3.2"
