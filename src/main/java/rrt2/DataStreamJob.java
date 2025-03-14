package rrt2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public final class DataStreamJob {

    private static final StreamExecutionEnvironment ENV = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final StreamTableEnvironment TABLE_ENV = StreamTableEnvironment.create(ENV);

    private static final String SQL_SELECT = "SELECT message FROM KafkaTable";
    private static final String KAFKA_TABLE_DDL = "CREATE TABLE KafkaTable (" +
            "`message` STRING" +
            ") WITH (" +
            "'connector'='kafka'," +
            "'topic'='my_topic2'," +
            "'properties.bootstrap.servers'='localhost:9092'," +
            "'properties.group.id'='flink-kafka-consumer-group'," +
            "'properties.enable.auto.commit'='true'," +
            "'properties.auto.commit.interval.ms'='500'," +
            "'format'='raw'," +
            "'scan.startup.mode'='group-offsets'" +
            ")";

    public static void main(String[] args) throws Exception {

        TABLE_ENV.executeSql(KAFKA_TABLE_DDL);
        final TableResult result = TABLE_ENV.executeSql(SQL_SELECT);
        result.print();
        ENV.execute("Flink Kafka SQL Example");

    }
}
