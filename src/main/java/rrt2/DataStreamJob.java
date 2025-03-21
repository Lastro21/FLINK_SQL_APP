package rrt2;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DataStreamJob {

    private static final String SQL_SOURCE_FILE = "/home/name/Desktop/application_test/FLINK_SQL_APP2/src/main/java/rrt2/flinkApplication.sql";
    private static final String SQL_PARAMS_FILE = "/home/name/Desktop/application_test/FLINK_SQL_APP2/src/main/java/rrt2/sqlParams.txt";
    private static final char SEPARATOR = '=';
    private static final Map<String, String> SQL_PARAMS = new HashMap<>();
    private static final EnvironmentSettings FLINK_ENV_SETTINGS = EnvironmentSettings.newInstance().inStreamingMode().build();
    private static final TableEnvironment FLINK_TABLE_ENV = TableEnvironment.create(FLINK_ENV_SETTINGS);

    public static void main(String[] args) throws Exception {

        fillParamsSQL();

        final String sourceSQL = new String(Files.readAllBytes(Paths.get(SQL_SOURCE_FILE)));
        final String resultSQL = replaceTemplate(sourceSQL);

        for (final String statement : resultSQL.split(";")) {
            FLINK_TABLE_ENV.executeSql(statement);
        }
    }

    public static Map<String, String> fillParamsSQL() {
        try (final BufferedReader br = new BufferedReader(new FileReader(SQL_PARAMS_FILE))) {
            String line;
            while ((line = br.readLine()) != null) {
                int charIndex = line.trim().indexOf(SEPARATOR);
                if (charIndex != -1) {
                    SQL_PARAMS.put(line.trim().substring(0, charIndex), line.trim().substring(charIndex + 1, line.length()));
                } else {
                    System.exit(1);
                }
            }
        } catch (final IOException exception) {
            exception.printStackTrace();
        }
        return SQL_PARAMS;
    }

    public static String replaceTemplate(final String content) {

        final Pattern pattern = Pattern.compile("\\{\\{(.*?)\\}\\}");
        final Matcher matcher = pattern.matcher(content);
        final StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            final String key = matcher.group(1);
            final String value = SQL_PARAMS.getOrDefault(key, "");
            matcher.appendReplacement(result, value);
        }
        matcher.appendTail(result);

        return result.toString();
    }
}
