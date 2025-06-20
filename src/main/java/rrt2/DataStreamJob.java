package rrt2;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public final class DataStreamJob {

    private static final EnvironmentSettings FLINK_ENV_SETTINGS = EnvironmentSettings.newInstance().inStreamingMode().build();
    private static final TableEnvironment FLINK_TABLE_ENV = TableEnvironment.create(FLINK_ENV_SETTINGS);

    public static void main(String[] args) throws Exception {

        final String resultSQL = "CREATE TABLE postgres_sink (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:postgresql://localhost:5432/postgres',\n" +
                "    'table-name' = 'your_table22',\n" +
                "    'username' = 'postgres',\n" +
                "    'password' = 'password',\n" +
                "    'driver' = 'org.postgresql.Driver'\n" +
                ");" +
                "INSERT INTO postgres_sink\n" +
                "VALUES (1, 'Test Name42', 'This is a test description42');";

        for (final String statement : resultSQL.split(";")) {
            FLINK_TABLE_ENV.executeSql(statement);
        }
    }
}
