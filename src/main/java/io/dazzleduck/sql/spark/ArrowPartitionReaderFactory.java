package io.dazzleduck.sql.spark;

import org.apache.arrow.flight.FlightInfo;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ArrowPartitionReaderFactory implements PartitionReaderFactory, Serializable {

    private final StructType requiredPartitionSchema;
    private final InternalRow requiredPartitions;
    private final StructType outputSchema;
    private final DatasourceOptions datasourceOptions;

    public ArrowPartitionReaderFactory(
            StructType outputSchema,
            StructType requiredPartitionSchema,
            InternalRow requiredPartitions,
            DatasourceOptions datasourceOptions) {
        this.outputSchema = Objects.requireNonNull(outputSchema, "outputSchema cannot be null");
        this.requiredPartitionSchema = Objects.requireNonNull(requiredPartitionSchema, "requiredPartitionSchema cannot be null");
        this.requiredPartitions = requiredPartitions;
        this.datasourceOptions = Objects.requireNonNull(datasourceOptions, "datasourceOptions cannot be null");
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        throw new UnsupportedOperationException("Row reader is not supported, use columnar reader");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        ArrowPartition arrowPartition = (ArrowPartition) partition;
        ByteBuffer buffer = ByteBuffer.wrap(arrowPartition.bytes());
        FlightInfo flightInfo;
        try {
            flightInfo = FlightInfo.deserialize(buffer);
        } catch (IOException | URISyntaxException e) {
            throw new IllegalStateException("Failed to deserialize FlightInfo from partition", e);
        }
        return new ArrowRpcReader(flightInfo, outputSchema, requiredPartitionSchema, requiredPartitions, datasourceOptions);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }
}
