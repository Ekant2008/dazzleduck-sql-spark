package io.dazzleduck.sql.spark;

import io.dazzleduck.sql.spark.extension.FieldReference;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.types.StructType;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

/**
 * The supported format is as following:
 * <p>
 * Aggregation:
 * SELECT count(*), key FROM ((inner_query_with_types) WHERE filters) GROUP BY key LIMIT 10;
 * <p>
 * Projection Only:
 * SELECT c1, c2 FROM ((inner_query_with_types) WHERE filters) LIMIT 10;
 */
public class QueryBuilderV2 {

    private QueryBuilderV2() {
    }

    public static String build(StructType datasourceSchema,
                               StructType partitionSchema,
                               DatasourceOptions datasourceOptions,
                               StructType outputSchema,
                               Expression[] pushedPredicates,
                               int limit,
                               DuckDBExpressionSQLBuilder dialect) {
        var source = buildSource(datasourceOptions, partitionSchema);
        var inner = DuckDBExpressionSQLBuilder.buildCast(datasourceSchema, source, dialect);
        var selectClause = stream(outputSchema.fieldNames())
                .map(f -> dialect.build(new FieldReference(new String[]{f})))
                .collect(Collectors.joining(", "));
        var whereClause = buildWhereClause(pushedPredicates, dialect);
        var limitClause = buildLimitClause(limit);
        return "SELECT %s FROM (%s)%s%s".formatted(selectClause, inner, whereClause, limitClause);
    }

    public static String buildForAggregation(StructType datasourceSchema,
                                             StructType partitionSchema,
                                             DatasourceOptions datasourceOptions,
                                             Expression[] pushedPredicates,
                                             Aggregation pushedAggregation,
                                             int limit,
                                             DuckDBExpressionSQLBuilder dialect) {
        var source = buildSource(datasourceOptions, partitionSchema);
        var inner = DuckDBExpressionSQLBuilder.buildCast(datasourceSchema, source, dialect);
        var selectClause = Stream.concat(
                        stream(pushedAggregation.groupByExpressions()),
                        stream(pushedAggregation.aggregateExpressions()))
                .map(dialect::build)
                .collect(Collectors.joining(", "));
        var whereClause = buildWhereClause(pushedPredicates, dialect);
        var groupByClause = buildGroupByClause(pushedAggregation, dialect);
        var limitClause = buildLimitClause(limit);
        return "SELECT %s FROM (%s)%s%s%s".formatted(selectClause, inner, whereClause, groupByClause, limitClause);
    }

    private static String buildWhereClause(Expression[] pushedPredicates, DuckDBExpressionSQLBuilder dialect) {
        if (pushedPredicates == null || pushedPredicates.length == 0) {
            return "";
        }
        return " WHERE " + stream(pushedPredicates).map(dialect::build).collect(Collectors.joining(" AND "));
    }

    private static String buildGroupByClause(Aggregation pushedAggregation, DuckDBExpressionSQLBuilder dialect) {
        if (pushedAggregation.groupByExpressions() == null || pushedAggregation.groupByExpressions().length == 0) {
            return "";
        }
        return " GROUP BY " + stream(pushedAggregation.groupByExpressions())
                .map(dialect::build)
                .collect(Collectors.joining(", "));
    }

    private static String buildLimitClause(int limit) {
        return limit < 0 ? "" : " LIMIT %d".formatted(limit);
    }

    private static String buildSource(DatasourceOptions options, StructType partitionSchema) {
        /* ===============================
           DuckLake: table-based
           =============================== */
        if (options.sourceType() == DatasourceOptions.SourceType.DUCKLAKE) {
            return "%s.%s.%s".formatted(
                    escapeIdentifier(options.catalog()),
                    escapeIdentifier(options.schema()),
                    escapeIdentifier(options.table()));
        }

        /* ===============================
           Hive: file-based
           =============================== */
        var path = escapeSqlString(options.path());

        if (options.partitionColumns().isEmpty()) {
            return "read_parquet('%s')".formatted(path);
        }

        String partition = "/*".repeat(options.partitionColumns().size()) + "/*.parquet";

        var hiveTypes = stream(partitionSchema.fields())
                .map(f -> "%s:%s".formatted(
                        escapeIdentifier(f.name()),
                        DuckDBExpressionSQLBuilder.translateDataType(f.dataType())))
                .collect(Collectors.joining(", "));

        return "read_parquet('%s%s', hive_types={%s}, union_by_name=true)".formatted(path, partition, hiveTypes);
    }

    private static String escapeSqlString(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "''");
    }

    private static String escapeIdentifier(String identifier) {
        if (identifier == null) {
            return "";
        }
        // DuckDB uses double quotes for identifiers
        return identifier.replace("\"", "\"\"");
    }
}