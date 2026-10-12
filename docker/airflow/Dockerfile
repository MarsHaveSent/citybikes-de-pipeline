FROM apache/spark:4.1.3-scala2.13-java17-ubuntu AS spark-dist

FROM apache/airflow:3.3.2-python3.12

# spark-submit запускает драйвер внутри контейнера Airflow, ему нужны JDK и дистрибутив Spark
USER root
COPY --from=spark-dist /opt/java/openjdk /opt/java/openjdk
COPY --from=spark-dist /opt/spark/bin /opt/spark/bin
COPY --from=spark-dist /opt/spark/jars /opt/spark/jars
COPY --from=spark-dist /opt/spark/python /opt/spark/python
USER airflow

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:$PATH

# Версия apache-airflow указана явно, чтобы pip не обновил сам Airflow вместе с провайдером
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "apache-airflow-providers-apache-spark==6.3.2"
