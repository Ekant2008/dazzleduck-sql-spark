
### Prerequisites
- Spark 3.5.6
- JDK 17
- Docker
### Getting started
- Start Dazzle duck server with `example/data` mounted at `/data` directory
`docker run -ti -v "$PWD/example/data":/local-data -p 59307:59307 -p 8080:8080 dazzleduck/dazzleduck --conf warehouse=/warehouse` 
- Start spark sql with package bin/spark-sql  --packages io.dazzleduck.sql:dazzleduck-sql-spark:0.0.4
- At the spark sql prompt 
`create temp view t ( key string, value string, p int) using io.dazzleduck.sql.spark.ArrowRPCTableProvider  
options ( url = 'jdbc:arrow-flight-sql://localhost:59307?disableCertificateVerification=true&user=admin&password=admin', partition_columns 'p', path '/local-data/parquet/kv', connection_timeout 'PT60m');`
- query the table with
`select * from t`

### started with Ducklake(run these query in startupscript)
- Install and load DuckLake extension
`INSTALL ducklake;`
`LOAD ducklake;`
- Attach DuckLake catalog
- Using the mounted data directory
`ATTACH 'ducklake:/warehouse/metadata' AS my_catalog (DATA_PATH '/warehouse/data');`
- Create a table in DuckLake
`CREATE TABLE catalog_name.schema_name.table_name (
key STRING,
value STRING,
partition INT
);`

- Set partitioning
`ALTER TABLE catalog_name.schema_name.table_name
SET PARTITIONED BY (partition);`

- Insert data
`INSERT INTO catalog_name.schema_name.table_name
VALUES
('k00', 'v00', 0),
('k01', 'v01', 0),
('k51', 'v51', 1),
('k61', 'v61', 1);`
### spark sql
- Start spark sql with package bin/spark-sql  --packages io.dazzleduck.sql:dazzleduck-sql-spark:0.0.4
- At the spark sql prompt 
`CREATE TEMP VIEW t (key string, value string, partition int)
USING io.dazzleduck.sql.spark.ArrowRPCTableProvider
OPTIONS (
  url 'jdbc:arrow-flight-sql://localhost:33335?useEncryption=false&disableCertificateVerification=true&user=admin&password=admin',
  identifier 'null',
  database 'catalog_name',
  schema 'schema_name',
  table 'table_name',
  username 'admin',
  password 'admin',
  partition_columns 'partition',
  connection_timeout 'PT10M'
)`
- query the table with
    `select * from t`