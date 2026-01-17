package io.dazzleduck.sql.spark;

import io.dazzleduck.sql.spark.extension.FieldReference;
import org.apache.spark.sql.connector.expressions.Cast;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Extract;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.execution.datasources.v2.V2ColumnUtils;
import org.apache.spark.sql.types.*;

import java.util.Optional;

public class AggregationUtil {

    private AggregationUtil() {
    }

    public static Optional<StructType> getSchemaForPushedAggregation(Aggregation aggregation, StructType schema) {
        var res = new StructType();
        Expression[] expressions = aggregation.groupByExpressions();
        if (expressions != null) {
            for (var expression : expressions) {
                if (expression == null) {
                    return Optional.empty();
                }
                if (expression instanceof NamedReference namedReference) {
                    var field = getField(schema, namedReference);
                    if (field.isEmpty()) {
                        return Optional.empty();
                    }
                    res = res.add(field.get());
                } else if (expression instanceof Cast c && c.expression() instanceof FieldReference f && f.fieldNames().length == 1) {
                    var name = String.join("_", f.fieldNames());
                    res = res.add(new StructField(String.format("cast(%s)", name), c.dataType(), true, Metadata.empty()));
                } else if (expression instanceof Extract e && (e.source() instanceof Cast || e.source() instanceof FieldReference)) {
                    var name = e.source().toString();
                    res = res.add(new StructField(String.format("extract(%s)", name), LongType$.MODULE$, true, Metadata.empty()));
                } else {
                    return Optional.empty();
                }
            }
        }
        for (var agg : aggregation.aggregateExpressions()) {
            if (agg instanceof Max || agg instanceof Min) {
                var p = processMinOrMax(agg, schema);
                if (p.isPresent()) {
                    res = res.add(p.get());
                } else {
                    return Optional.empty();
                }
            } else if (agg instanceof Count count) {
                if (V2ColumnUtils.extractV2Column(count.column()).isDefined() && !count.isDistinct()) {
                    var columnName = V2ColumnUtils.extractV2Column(count.column()).get();
                    var f = new StructField(String.format("cast(count(%s) as long)", columnName), DataTypes.LongType, true,
                            Metadata.empty());
                    res = res.add(f);
                } else {
                    return Optional.empty();
                }
            } else if (agg instanceof CountStar) {
                var f = new StructField("cast(count(*) as long)", DataTypes.LongType, true, Metadata.empty());
                res = res.add(f);
            } else if (agg instanceof Sum sum) {
                if (V2ColumnUtils.extractV2Column(sum.column()).isDefined()) {
                    var columnName = V2ColumnUtils.extractV2Column(sum.column()).get();
                    var fieldName = String.format("sum(%s)", columnName);
                    StructField f;
                    var col = schema.apply(columnName);
                    if (col.dataType() instanceof LongType || col.dataType() instanceof IntegerType) {
                        f = new StructField(fieldName, new DecimalType(), false, Metadata.empty());
                    } else if (col.dataType() instanceof ShortType || col.dataType() instanceof ByteType) {
                        f = new StructField(fieldName, DataTypes.LongType, false, Metadata.empty());
                    } else if (col.dataType() instanceof FloatType || col.dataType() instanceof DoubleType) {
                        f = new StructField(fieldName, DataTypes.DoubleType, false, Metadata.empty());
                    } else {
                        f = new StructField(fieldName, col.dataType(), false, Metadata.empty());
                    }
                    res = res.add(f);
                } else {
                    return Optional.empty();
                }
            } else if (agg instanceof Avg avg) {
                if (V2ColumnUtils.extractV2Column(avg.column()).isDefined()) {
                    var columnName = V2ColumnUtils.extractV2Column(avg.column()).get();
                    var fieldName = String.format("avg(%s)", columnName);
                    StructField f = new StructField(fieldName, DataTypes.DoubleType, false, Metadata.empty());
                    res = res.add(f);
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.empty();
            }
        }

        if (res.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(res);
        }
    }

    private static Optional<StructField> processMinOrMax(AggregateFunc agg, StructType schema) {
        String[] columnNames;
        String aggType;
        if (agg instanceof Min min) {
            columnNames = extractV2Column(min.column()).orElse(null);
            aggType = "min";
        } else if (agg instanceof Max max) {
            columnNames = extractV2Column(max.column()).orElse(null);
            aggType = "max";
        } else {
            return Optional.empty();
        }

        if (columnNames == null || columnNames.length == 0) {
            return Optional.empty();
        }

        StructField field = null;
        DataType currentType = schema;
        for (String columnName : columnNames) {
            if (!(currentType instanceof StructType structType)) {
                return Optional.empty();
            }
            try {
                field = structType.apply(columnName);
                currentType = field.dataType();
            } catch (Exception e) {
                return Optional.empty();
            }
        }

        if (field == null) {
            return Optional.empty();
        }

        var name = aggType + "(" + field.name() + ")";
        var dt = field.dataType();

        if (dt instanceof BooleanType ||
                dt instanceof ByteType ||
                dt instanceof ShortType ||
                dt instanceof IntegerType ||
                dt instanceof LongType ||
                dt instanceof FloatType ||
                dt instanceof DoubleType ||
                dt instanceof DecimalType ||
                dt instanceof DateType ||
                dt instanceof StringType ||
                dt instanceof TimestampType ||
                dt instanceof TimestampNTZType) {
            return Optional.of(field.copy(name, field.dataType(), field.nullable(), field.metadata()));
        }
        return Optional.empty();
    }

    public static Optional<StructField> getField(StructType schema, NamedReference reference) {
        String[] fieldNames = reference.fieldNames();
        if (fieldNames == null || fieldNames.length == 0) {
            return Optional.empty();
        }

        DataType currentType = schema;
        StructField result = null;
        for (String fieldName : fieldNames) {
            if (!(currentType instanceof StructType structType)) {
                return Optional.empty();
            }
            try {
                result = structType.apply(fieldName);
                currentType = result.dataType();
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(result);
    }

    private static Optional<String[]> extractV2Column(Expression expression) {
        if (expression instanceof NamedReference nr) {
            return Optional.of(nr.fieldNames());
        }
        return Optional.empty();
    }
}
