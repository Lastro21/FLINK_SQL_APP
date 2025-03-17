-- Создание таблицы KafkaTable
CREATE TABLE KafkaTable (
id INT,
message STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'my_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'flink-kafka-consumer-group',
      'properties.enable.auto.commit' = 'true',
      'properties.auto.commit.interval.ms' = '500',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true',
      'scan.startup.mode' = 'latest-offset'
      );

-- Создание таблицы postgresTable
CREATE TABLE postgresTable (
id INT,
message STRING,
PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'jdbc',
      'url' = 'jdbc:postgresql://localhost:5432/postgres',
      'table-name' = 'real_table',
      'username' = 'postgres',
      'password' = 'postgres'
      );

-- Создание временного представления FilteredKafkaTable
CREATE TEMPORARY VIEW FilteredKafkaTable AS
SELECT
    CASE
        WHEN CAST(id AS INT) IS NOT NULL THEN CAST(id AS INT)
        ELSE NULL
        END AS id,
    message
FROM KafkaTable
WHERE CAST(id AS INT) IS NOT NULL;

-- Вставка данных из FilteredKafkaTable в postgresTable
INSERT INTO postgresTable
SELECT id, message
FROM FilteredKafkaTable;