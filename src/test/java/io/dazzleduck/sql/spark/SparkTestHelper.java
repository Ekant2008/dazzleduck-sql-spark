package io.dazzleduck.sql.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class SparkTestHelper {

    public static void assertEqual(SparkSession sparkSession, String expectedSql, String resultSql) {
        assertEqual(sparkSession, expectedSql, resultSql, false);
    }
    public static void assertEqual(SparkSession sparkSession, String expectedSql, String resultSql, boolean detailedError) {

        Dataset<Row> expected = sparkSession.sql(expectedSql);
        Dataset<Row> result = sparkSession.sql(resultSql);

        Row[] e = (Row[]) expected.collect();
        Row[] r = (Row[]) result.collect();

        assertArrayEquals(e, r, detailedError ? buildErrorMessage(expectedSql, resultSql, expected, result) : "Result does not match");
    }

    private static String buildErrorMessage(String expectedSql, String resultSql, Dataset<Row> expected, Dataset<Row> result) {

        return String.format("""
                Expected SQL:
                %s
                
                Result SQL:
                %s
                
                Expected Result:
                %s
                
                Actual Result:
                %s
                """, expectedSql, resultSql, datasetToString(expected), datasetToString(result));
    }

    private static String datasetToString(Dataset<Row> ds) {
        return "Schema:\n" + ds.schema().treeString() + "\nRows:\n" + Arrays.toString((long[]) ds.collect());
    }
}
