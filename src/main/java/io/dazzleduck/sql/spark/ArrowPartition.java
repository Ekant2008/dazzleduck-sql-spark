package io.dazzleduck.sql.spark;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a partition containing serialized Arrow Flight information.
 */
public record ArrowPartition(byte[] bytes) implements InputPartition, Serializable {

    public ArrowPartition {
        Objects.requireNonNull(bytes, "bytes cannot be null");
        // Defensive copy for immutability
        bytes = Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public byte[] bytes() {
        // Return defensive copy
        return Arrays.copyOf(bytes, bytes.length);
    }
}
