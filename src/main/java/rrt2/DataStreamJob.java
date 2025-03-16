package rrt2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public final class DataStreamJob {

    private static final StreamExecutionEnvironment ENV = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final StreamTableEnvironment TABLE_ENV = StreamTableEnvironment.create(ENV);

    public static void main(String[] args) throws Exception {

        final String KAFKA_TABLE_DDL = "CREATE TABLE KafkaTable (" +
                "  id INT," +
                "`message` STRING" +
                ") WITH (" +
                "'connector'='kafka'," +
                "'topic'='my_topic'," +
                "'properties.bootstrap.servers'='localhost:9092'," +
                "'properties.group.id'='flink-kafka-consumer-group'," +
                "'properties.enable.auto.commit'='true'," +
                "'properties.auto.commit.interval.ms'='500'," +
                "'format'='json'," +
                "'json.ignore-parse-errors'='true'," +
                "'scan.startup.mode'='latest-offset'" +
                ")";
        TABLE_ENV.executeSql(KAFKA_TABLE_DDL);

        final String postgresSinkDDL = "CREATE TABLE postgresTable (" +
                "id INT," +
                "message STRING," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "'connector'='jdbc'," +
                "'url'='jdbc:postgresql://localhost:5432/postgres'," +
                "'table-name'='real_table'," +
                "'username'='postgres'," +
                "'password'='postgres'" +
                ")";
        TABLE_ENV.executeSql(postgresSinkDDL);

        // Функция для проверки и преобразования id в INT
        Table resultTable = TABLE_ENV.sqlQuery(
                "SELECT " +
                        "CASE " +
                        "  WHEN CAST(id AS INT) IS NOT NULL THEN CAST(id AS INT) " +
                        "  ELSE NULL " +
                        "END AS id, " +
                        "message " +
                        "FROM KafkaTable " +
                        "WHERE CAST(id AS INT) IS NOT NULL"
        );

        TABLE_ENV.createTemporaryView("FilteredKafkaTable", resultTable);

        final String insertQuery = "INSERT INTO postgresTable SELECT id, message FROM FilteredKafkaTable";
        TABLE_ENV.executeSql(insertQuery);

        ENV.execute("Flink Kafka SQL Example");
    }
}
