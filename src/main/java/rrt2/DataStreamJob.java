package rrt2;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class DataStreamJob {

    private static final Logger LOGGER = Logger.getLogger(String.valueOf(DataStreamJob.class));

    private static final String SQL_SOURCE_FILE = "/home/name/Desktop/application_test/FLINK_SQL_APP2/src/main/java/rrt2/flinkApplication.sql";
    private static final String SQL_PARAMS_FILE = "/home/name/Desktop/application_test/FLINK_SQL_APP2/src/main/java/rrt2/sqlParams.txt";
    private static final char SEPARATOR = '=';
    private static final Map<String, String> SQL_PARAMS = new HashMap<>();
    private static final EnvironmentSettings FLINK_ENV_SETTINGS = EnvironmentSettings.newInstance().inStreamingMode().build();
    private static final TableEnvironment FLINK_TABLE_ENV = TableEnvironment.create(FLINK_ENV_SETTINGS);

    public static void main(final String[] args) throws Exception {

        initMetrics();

        fillParamsSQL();

        String sourceSQL;

        try (final InputStream inputStream = Files.newInputStream(Paths.get(SQL_SOURCE_FILE));
             final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            sourceSQL = reader.lines().collect(Collectors.joining("\n"));
        } catch (final IOException exception) {
            throw new RuntimeException("ERROR reading file SQL_SOURCE_FILE : " + SQL_SOURCE_FILE, exception);
        }

        final String resultSQL = replaceTemplate(sourceSQL);

        for (final String statement : resultSQL.split(";")) {
            FLINK_TABLE_ENV.executeSql(statement);
        }
    }

    public static void initMetrics() throws IOException {
        JvmMetrics.builder().register();

        final HTTPServer server = HTTPServer.builder()
                .port(9400)
                .buildAndStart();

        LOGGER.info("HTTPServer listening on http://localhost:" + server.getPort() + "/metrics");
    }

    public static Map<String, String> fillParamsSQL() {
        try (final BufferedReader br = new BufferedReader(new FileReader(SQL_PARAMS_FILE))) {
            String line;
            while ((line = br.readLine()) != null) {
                int charIndex = line.trim().indexOf(SEPARATOR);
                if (charIndex != -1) {
                    SQL_PARAMS.put(line.trim().substring(0, charIndex), line.trim().substring(charIndex + 1, line.length()));
                } else {
                    throw new RuntimeException("ERROR reading file SQL_PARAMS_FILE");
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
