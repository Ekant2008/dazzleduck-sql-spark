package io.dazzleduck.sql.spark.extension;

import org.apache.spark.sql.connector.expressions.NamedReference;

import java.util.Arrays;
import java.util.Objects;

public record FieldReference(String[] parts) implements NamedReference {

    public FieldReference {
        Objects.requireNonNull(parts, "parts cannot be null");
        // Defensive copy for immutability
        parts = Arrays.copyOf(parts, parts.length);
    }

    @Override
    public String[] fieldNames() {
        return Arrays.copyOf(parts, parts.length);
    }
}
