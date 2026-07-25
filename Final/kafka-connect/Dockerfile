FROM confluentinc/cp-kafka-connect:7.6.1

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6

COPY plugins/postgresql-42.7.4.jar \
  /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.4.jar
COPY plugins/confluentinc-kafka-connect-hdfs-10.2.17/ \
  /usr/share/confluent-hub-components/confluentinc-kafka-connect-hdfs/
COPY plugins/hdfs-service-loader.jar \
  /usr/share/confluent-hub-components/confluentinc-kafka-connect-hdfs/lib/hdfs-service-loader.jar

USER appuser
