package rrt2;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;


public final class DataStreamJob {

    public static void main(String[] args) throws Exception {

        final EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        final String sqlFilePath = "/home/your_path/Desktop/application_test/FLINK_SQL_APP/src/main/java/rrt2/flink_application.sql";
        final String sql = new String(Files.readAllBytes(Paths.get(sqlFilePath)));

        for (final String statement : sql.split(";")) {
            tableEnv.executeSql(statement);
        }

    }
}
