package rrt2;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

public final class DataStreamJob {

    private static final EnvironmentSettings FLINK_ENV_SETTINGS = EnvironmentSettings.newInstance().inStreamingMode().build();
    private static final TableEnvironment FLINK_TABLE_ENV = TableEnvironment.create(FLINK_ENV_SETTINGS);

    public static void main(String[] args) throws Exception {

        // 1. Создание таблицы с JDBC коннектором
        FLINK_TABLE_ENV.executeSql(
                "CREATE TABLE postgres_sink (\n" +
                        "    id INT,\n" +
                        "    name STRING,\n" +
                        "    description STRING,\n" +
                        "    PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "    'connector' = 'jdbc',\n" +
                        "    'url' = 'jdbc:postgresql://localhost:5432/postgres',\n" +
                        "    'table-name' = 'your_table22',\n" +
                        "    'username' = 'postgres',\n" +
                        "    'password' = 'password*',\n" +
                        "    'driver' = 'org.postgresql.Driver'\n" +
                        ")"
        );

// 2. Вставка данных через Table API (без SQL)
        Table dataTable = FLINK_TABLE_ENV.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("description", DataTypes.STRING())
                ),
                Row.of(5, "Test Name551", "This is a test description5")
        );

// 3. Выполнение вставки
        dataTable.executeInsert("postgres_sink");
    }
}
