package io.dazzleduck.sql.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class SparkTestHelper {

    public static void assertEqual(SparkSession sparkSession, String expectedSql, String resultSql) {
        assertEqual(sparkSession, expectedSql, resultSql, false);
    }
    public static void assertEqual(SparkSession sparkSession, String expectedSql, String resultSql, boolean detailedError) {
        var expected = sparkSession.sql(expectedSql);
        var result = sparkSession.sql(resultSql);
        Row[] e = (Row[]) expected.collect();
        Row[] r = (Row[]) result.collect();
        assertArrayEquals(e, r, detailedError ? String.format("\nExpected SQL: %s\nResult SQL: %s\nExpected rows: %d\nResult rows: %d",
                expectedSql, resultSql, e.length, r.length) : "Result do not match");
    }
}
