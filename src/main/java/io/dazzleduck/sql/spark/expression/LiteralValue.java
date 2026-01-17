package io.dazzleduck.sql.spark.expression;

import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;

public record LiteralValue<T>(T value, DataType dataType) implements Literal<T> {
    @Override
    public String toString() {
        if (value == null) {
            return "NULL";
        }
        if (dataType instanceof StringType) {
            // Escape single quotes to prevent SQL injection
            String escaped = value.toString().replace("'", "''");
            return "'%s'".formatted(escaped);
        }
        return value.toString();
    }
}
