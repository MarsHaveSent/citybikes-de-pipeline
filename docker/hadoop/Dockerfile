FROM apache/hadoop:3.5.0

# Named volume наследует владельца каталога из образа. Без этого том создаётся от root,
# и процессы HDFS от пользователя hadoop не могут в него писать.
USER root
RUN mkdir -p /hadoop/dfs && chown hadoop:users /hadoop/dfs
USER hadoop
