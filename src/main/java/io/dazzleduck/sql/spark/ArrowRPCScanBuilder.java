package io.dazzleduck.sql.spark;

import org.apache.arrow.flight.FlightInfo;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class ArrowRPCScanBuilder implements ScanBuilder,
        SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns,
        SupportsPushDownLimit,
        SupportsPushDownAggregates {

    private static final Logger logger = LoggerFactory.getLogger(ArrowRPCScanBuilder.class);
    private static final int NO_LIMIT = -1;

    private final StructType sourceSchema;
    private final DuckDBExpressionSQLBuilder dialect;
    private final DatasourceOptions datasourceOptions;
    private final StructType sourcePartitionSchema;

    private StructType outputSchema;
    private Predicate[] pushedPredicates;
    private int limit = NO_LIMIT;
    private Aggregation pushedAggregation = null;
    private FlightInfo flightInfo;
    private boolean completePushedPredicates = false;

    public ArrowRPCScanBuilder(StructType sourceSchema, DatasourceOptions datasourceOptions) {
        this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema cannot be null");
        this.datasourceOptions = Objects.requireNonNull(datasourceOptions, "datasourceOptions cannot be null");
        this.dialect = new DuckDBExpressionSQLBuilder(sourceSchema);
        this.outputSchema = sourceSchema;
        this.sourcePartitionSchema = getPartitionSchema(sourceSchema, datasourceOptions.partitionColumns());
    }

    @Override
    public Scan build() {
        try {
            FlightInfo flightInfoToSend;
            if (flightInfo == null) {
                var queryObject = QueryBuilderV2.build(
                        sourceSchema, sourcePartitionSchema, datasourceOptions,
                        outputSchema, pushedPredicates, limit, dialect);
                flightInfoToSend = FlightSqlClientPool.INSTANCE.getInfo(datasourceOptions, queryObject);
            } else {
                flightInfoToSend = flightInfo;
            }
            return new ArrowRPCScan(
                    outputSchema,
                    pushedAggregation != null,
                    new StructType(),
                    InternalRow.empty(),
                    datasourceOptions,
                    flightInfoToSend);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build ArrowRPCScan", e);
        }
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        outputSchema = requiredSchema;
    }

    @Override
    public boolean pushLimit(int limit) {
        this.limit = limit;
        return true;
    }

    @Override
    public boolean isPartiallyPushed() {
        return limit >= 0;
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        var pushedPredicateList = new ArrayList<Predicate>();
        var notPushedList = new ArrayList<Predicate>();

        for (var predicate : predicates) {
            var result = compileExpression(predicate, dialect);
            if (result.isDefined()) {
                pushedPredicateList.add(predicate);
            } else {
                notPushedList.add(predicate);
            }
        }

        this.pushedPredicates = pushedPredicateList.toArray(new Predicate[0]);
        this.completePushedPredicates = pushedPredicates.length == predicates.length;
        return notPushedList.toArray(new Predicate[0]);
    }

    @Override
    public Predicate[] pushedPredicates() {
        if (pushedPredicates == null) {
            return new Predicate[0];
        }
        return Arrays.copyOf(pushedPredicates, pushedPredicates.length);
    }

    @Override
    public boolean supportCompletePushDown(Aggregation aggregation) {
        if (pushedPredicates != null && pushedPredicates.length > 0 && !completePushedPredicates) {
            return false;
        }

        var pushedAggregationSchema = AggregationUtil.getSchemaForPushedAggregation(aggregation, sourceSchema);
        if (pushedAggregationSchema.isEmpty()) {
            return false;
        }

        var queryObject = QueryBuilderV2.buildForAggregation(
                sourceSchema, sourcePartitionSchema, datasourceOptions,
                pushedPredicates, aggregation, limit, dialect);
        this.flightInfo = FlightSqlClientPool.INSTANCE.getInfo(datasourceOptions, queryObject);

        if (flightInfo.getEndpoints().size() == 1) {
            outputSchema = pushedAggregationSchema.get();
            pushedAggregation = aggregation;
            return true;
        }
        return false;
    }

    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        if (pushedAggregation != null) {
            if (pushedAggregation != aggregation) {
                throw new IllegalStateException(
                        "Attempting to push different aggregation than previously supported");
            }
            return true;
        }

        var pushedAggregationSchema = AggregationUtil.getSchemaForPushedAggregation(aggregation, sourceSchema);
        if (pushedAggregationSchema.isPresent()) {
            outputSchema = pushedAggregationSchema.get();
            pushedAggregation = aggregation;
            return true;
        }
        return false;
    }

    private static Option<String> compileExpression(Expression expression,
                                                    V2ExpressionSQLBuilder expressionSQLBuilder) {
        try {
            String built = expressionSQLBuilder.build(expression);
            return Option.apply(built);
        } catch (Exception e) {
            logger.debug("Failed to compile expression {}: {}", expression, e.getMessage());
            return Option.empty();
        }
    }

    private static StructType getPartitionSchema(StructType schema, List<String> partitionColumns) {
        var partitionColumnSet = new HashSet<>(partitionColumns);
        var fields = Arrays.stream(schema.fields())
                .filter(f -> partitionColumnSet.contains(f.name()))
                .toArray(StructField[]::new);
        return new StructType(fields);
    }
}
