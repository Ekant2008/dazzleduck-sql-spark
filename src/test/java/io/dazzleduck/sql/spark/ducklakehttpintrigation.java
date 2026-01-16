package io.dazzleduck.sql.spark;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ducklakehttpintrigation {



    private static SparkSession spark;
    private static Path workspace;

    private static final int PORT = 33335;
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";

    private static final String CATALOG = "test_ducklake";
    private static final String SCHEMA = "main";
    private static final String TABLE = "tt_p";

    private static final String RPC_TABLE = "rpc_tt_p";
    private static final String URL = "jdbc:arrow-flight-sql://localhost:" + PORT + "?useEncryption=false&disableCertificateVerification=true&disableSessionCatalog=true" + "&user=" + USER + "&password=" + PASSWORD;
    private static final String SCHEMA_DDL = "key string, value string, partition int";

    @BeforeAll
    static void setup() throws Exception {

        workspace = Files.createTempDirectory("ducklake_rpc_test_");

        ConnectionPool.executeBatch(new String[]{
                "INSTALL ducklake",
                "LOAD ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')".formatted(workspace, CATALOG, workspace)
        });

        ConnectionPool.execute("""
            CREATE TABLE %s.%s.%s (
              key string,
              value string,
              partition int )
            """.formatted(CATALOG, SCHEMA, TABLE));

        ConnectionPool.execute("""
            ALTER TABLE %s.%s.%s
            SET PARTITIONED BY (partition)
            """.formatted(CATALOG, SCHEMA, TABLE));

        ConnectionPool.execute("""
            INSERT INTO %s.%s.%s VALUES
              ('k00','v00',0),
              ('k01','v01',0),
              ('k51','v51',1),
              ('k61','v61',1)
            """.formatted(CATALOG, SCHEMA, TABLE));

        var config = ConfigFactory.load();

        spark = SparkInitializationHelper.createSparkSession(config);
        DuckDBInitializationHelper.initializeDuckDB(config);
        FlightTestUtil.createFsServiceAnsStartHttp(PORT);


    }


    @Test
    void testDuckLakeHttp() {
        String sql = """
        CREATE TEMP VIEW ducklake_http (key STRING, value STRING, partition INT)
        USING %s
        OPTIONS (
          url 'http://localhost:%d',
          protocol 'http',
          database '%s',
          schema '%s',
          table '%s',
          username 'admin',
          password 'admin',
          partition_columns 'partition',
          connection_timeout 'PT10M'
        )
        """.formatted(
                ArrowRPCTableProvider.class.getName(),
                PORT,
                CATALOG,
                SCHEMA,
                TABLE
        );

        spark.sql(sql);

        //spark.sql("SELECT * FROM ducklake_http").show();

        long sparkCount = spark.sql(String.format("SELECT count(*) FROM ducklake_http")).first().getLong(0);
        Assertions.assertEquals(2,sparkCount);
    }


    @AfterAll
    static void cleanup() throws IOException {
        spark.close();
        ConnectionPool.execute("DETACH " + CATALOG);
    }
}


