package io.dazzleduck.sql.spark;

import io.dazzleduck.sql.spark.extension.FieldReference;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects$;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.stream.Collectors;

public class DuckDBExpressionSQLBuilder extends V2ExpressionSQLBuilder {
    private final StructType schema;
    private final JdbcDialect jdbcDialect = JdbcDialects$.MODULE$.get("jdbc:postgresql");

    public DuckDBExpressionSQLBuilder(StructType schema) {
        this.schema = schema;
    }

    @Override
    public String visitLiteral(Literal literal) {
        return jdbcDialect.compileValue(CatalystTypeConverters.convertToScala(literal.value(), literal.dataType()))
                .toString();
    }

    @Override
    public String visitNamedReference(NamedReference namedReference) {
        return Arrays.stream(namedReference.fieldNames())
                .map(jdbcDialect::quoteIdentifier)
                .collect(Collectors.joining("."));
    }

    public String visitCast(String expression, DataType dataType) {
        return buildCast(expression, dataType);
    }

    @Override
    protected String visitSQLFunction(String funcName, String[] inputs) {
        if (jdbcDialect.isSupportedFunction(funcName)) {
            return super.visitSQLFunction(funcName, inputs);
        } else {
            throw new UnsupportedOperationException("Unsupported function: " + funcName);
        }
    }

    @Override
    public String visitAggregateFunction(String funcName, boolean isDistinct, String[] inputs) {
        if (!jdbcDialect.isSupportedFunction(funcName)) {
            throw new UnsupportedOperationException("Unsupported aggregate function: " + funcName);
        }
        if (isDistinct) {
            String args = String.join(", ", inputs);
            return "%s(DISTINCT %s)".formatted(funcName, args);
        }
        return super.visitSQLFunction(funcName, inputs);
    }

    public String buildCast(String expression, DataType dataType) {
        String sqlType = translateToSqlType(dataType);
        return "CAST(%s as %s)".formatted(expression, sqlType);
    }

    private String translateToSqlType(DataType dataType) {
        if (dataType instanceof StructType s) {
            var fields = s.fields();
            String inner = Arrays.stream(fields).map(f -> {
                String cast = translateToSqlType(f.dataType());
                return jdbcDialect.quoteIdentifier(f.name()) + " " + cast;
            }).collect(Collectors.joining(", "));
            return "STRUCT(%s)".formatted(inner);
        } else if (dataType instanceof MapType m) {
            String keyType = translateToSqlType(m.keyType());
            String valueType = translateToSqlType(m.valueType());
            return "MAP(%s, %s)".formatted(keyType, valueType);
        } else if (dataType instanceof ArrayType a) {
            String childCast = translateToSqlType(a.elementType());
            return "%s[]".formatted(childCast);
        } else if (dataType instanceof DecimalType d) {
            return "DECIMAL(%s, %s)".formatted(d.precision(), d.scale());
        } else if (dataType instanceof TimestampType) {
            return "TIMESTAMP";
        } else if (dataType instanceof TimestampNTZType) {
            return "TIMESTAMP";
        } else if (dataType instanceof StringType) {
            return "VARCHAR";
        } else if (dataType instanceof IntegerType) {
            return "INTEGER";
        } else if (dataType instanceof LongType) {
            return "BIGINT";
        } else if (dataType instanceof ShortType) {
            return "SMALLINT";
        } else if (dataType instanceof ByteType) {
            return "TINYINT";
        } else if (dataType instanceof DoubleType) {
            return "DOUBLE";
        } else if (dataType instanceof FloatType) {
            return "FLOAT";
        } else if (dataType instanceof BooleanType) {
            return "BOOLEAN";
        } else if (dataType instanceof BinaryType) {
            return "BLOB";
        } else if (dataType instanceof DateType) {
            return "DATE";
        } else {
            throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }
    }

    public static String translateSchema(StructType st) {
        return Arrays.stream(st.fields()).map(structField -> {
            var fname = structField.name();
            return fname + " " + translateDataType(structField.dataType());
        }).collect(Collectors.joining(","));
    }

    public static String buildCast(StructType dataType, String source, DuckDBExpressionSQLBuilder dialect) {
        var prefix = Arrays.stream(dataType.fields())
                .map(f -> "NULL::%s".formatted(translateDataType(f.dataType())))
                .collect(Collectors.joining(","));
        var suffix = Arrays.stream(dataType.fieldNames())
                .map(f -> dialect.build(new FieldReference(new String[]{f})))
                .collect(Collectors.joining(", "));
        return "FROM (VALUES(%s)) t(%s)\n".formatted(prefix, suffix) +
                "WHERE false\n" +
                "UNION ALL BY NAME\n" +
                "FROM %s".formatted(source);
    }

    public static String translateDataType(DataType dataType) {
        if (dataType instanceof IntegerType) {
            return "int";
        } else if (dataType instanceof LongType) {
            return "bigint";
        } else if (dataType instanceof ShortType) {
            return "smallint";
        } else if (dataType instanceof ByteType) {
            return "tinyint";
        } else if (dataType instanceof DoubleType) {
            return "double";
        } else if (dataType instanceof FloatType) {
            return "float";
        } else if (dataType instanceof BooleanType) {
            return "boolean";
        } else if (dataType instanceof StringType) {
            return "varchar";
        } else if (dataType instanceof BinaryType) {
            return "blob";
        } else if (dataType instanceof TimestampType) {
            return "timestamp";
        } else if (dataType instanceof TimestampNTZType) {
            return "timestamp";
        } else if (dataType instanceof DateType) {
            return "date";
        } else if (dataType instanceof DecimalType d) {
            return "decimal(%d,%d)".formatted(d.precision(), d.scale());
        } else if (dataType instanceof ArrayType arrayType) {
            return translateDataType(arrayType.elementType()) + "[]";
        } else if (dataType instanceof MapType m) {
            String keyType = translateDataType(m.keyType());
            String valueType = translateDataType(m.valueType());
            return "MAP(%s, %s)".formatted(keyType, valueType);
        } else if (dataType instanceof StructType st) {
            String inner = Arrays.stream(st.fields())
                    .map(field -> field.name() + " " + translateDataType(field.dataType()))
                    .collect(Collectors.joining(", "));
            return "STRUCT(%s)".formatted(inner);
        } else {
            throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }
    }
}
