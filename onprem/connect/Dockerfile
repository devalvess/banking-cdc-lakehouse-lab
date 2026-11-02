# Kafka Connect com Debezium. O driver JDBC do Db2 (jcc) não vem na imagem oficial por
# licenciamento da IBM — é adicionado aqui, a partir do Maven Central.
FROM quay.io/debezium/connect:2.7.3.Final

RUN curl -sfL -o /kafka/connect/debezium-connector-db2/jcc-11.5.9.0.jar \
    https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar
