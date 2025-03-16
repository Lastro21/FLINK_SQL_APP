mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-quickstart-java -DarchetypeVersion=1.20.1 -DgroupId=rrt1 -DartifactId=testSql -Dversion=0.1 -Dpackage=rrt2 -DinteractiveMode=false


==============================================================
flink_application.sql

-- Создаем таблицу для чтения данных из Kafka
CREATE TABLE kafka_source (
id STRING,
message STRING
) WITH (
'connector' = 'kafka',
'topic' = 'input_topic',
'properties.bootstrap.servers' = 'localhost:9092',
'format' = 'json',
'json.fail-on-missing-field' = 'false',
'json.ignore-parse-errors' = 'true'
);

-- Создаем таблицу для вывода данных в лог
CREATE TABLE print_sink (
id STRING,
message STRING
) WITH (
'connector' = 'print'
);

-- Переносим данные из kafka_source в print_sink
INSERT INTO print_sink
SELECT id, message FROM kafka_source;

==============================================================

pom.xml

<project xmlns="http://maven.apache.org/POM/4.0.0"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
<modelVersion>4.0.0</modelVersion>
<groupId>org.example</groupId>
<artifactId>flink-sql-kafka</artifactId>
<version>1.0-SNAPSHOT</version>
<properties>
<flink.version>1.20.1</flink.version>
<java.version>17</java.version>
</properties>
<dependencies>
<dependency>
<groupId>org.apache.flink</groupId>
<artifactId>flink-table-api-java-bridge_2.12</artifactId>
<version>${flink.version}</version>
</dependency>
<dependency>
<groupId>org.apache.flink</groupId>
<artifactId>flink-table-planner-blink_2.12</artifactId>
<version>${flink.version}</version>
</dependency>
<dependency>
<groupId>org.apache.flink</groupId>
<artifactId>flink-connector-kafka_2.12</artifactId>
<version>${flink.version}</version>
</dependency>
</dependencies>
<build>
<plugins>
<plugin>
<groupId>org.apache.maven.plugins</groupId>
<artifactId>maven-compiler-plugin</artifactId>
<version>3.8.1</version>
<configuration>
<source>${java.version}</source>
<target>${java.version}</target>
</configuration>
</plugin>
<plugin>
<groupId>org.apache.maven.plugins</groupId>
<artifactId>maven-shade-plugin</artifactId>
<version>3.2.4</version>
<executions>
<execution>
<phase>package</phase>
<goals>
<goal>shade</goal>
</goals>
<configuration>
<transformers>
<transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
<transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
<mainClass>org.example.FlinkSQLKafka</mainClass>
</transformer>
</transformers>
</configuration>
</execution>
</executions>
</plugin>
</plugins>
</build>
</project>

==============================================================

FlinkSQLKafka.java

package org.example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;

public class FlinkSQLKafka {
public static void main(String[] args) throws Exception {
EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

        String sqlFilePath = "src/main/resources/flink_application.sql";
        String sql = new String(Files.readAllBytes(Paths.get(sqlFilePath)));
        
        for (String statement : sql.split(";")) {
            tableEnv.executeSql(statement);
        }
    }
}