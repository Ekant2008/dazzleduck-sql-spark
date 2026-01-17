package io.dazzleduck.sql.spark;

import org.apache.arrow.flight.FlightInfo;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ArrowRPCScan implements Scan, Batch {

    private final StructType requiredPartitionSchema;
    private final InternalRow requiredPartitions;
    private final DatasourceOptions datasourceOptions;
    private final StructType outputSchema;
    private final boolean pushedAggregation;
    private final FlightInfo flightInfo;

    public ArrowRPCScan(StructType outputSchema,
                        boolean pushedAggregation,
                        StructType requiredPartitionSchema,
                        InternalRow requiredPartitions,
                        DatasourceOptions datasourceOptions,
                        FlightInfo flightInfo) {
        this.outputSchema = Objects.requireNonNull(outputSchema, "outputSchema cannot be null");
        this.pushedAggregation = pushedAggregation;
        this.requiredPartitionSchema = Objects.requireNonNull(requiredPartitionSchema, "requiredPartitionSchema cannot be null");
        this.requiredPartitions = requiredPartitions;
        this.datasourceOptions = Objects.requireNonNull(datasourceOptions, "datasourceOptions cannot be null");
        this.flightInfo = Objects.requireNonNull(flightInfo, "flightInfo cannot be null");
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return Arrays.stream(withNewEndpoints(flightInfo)).map(e -> {
                    ByteBuffer buffer = e.serialize();
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    return new ArrowPartition(bytes);
                }).toArray(InputPartition[]::new);
    }

    private static FlightInfo[] withNewEndpoints(FlightInfo flightInfo ) {
        return flightInfo
                .getEndpoints()
                .stream()
                .map( e ->
                        new FlightInfo(flightInfo.getSchema(),
                                flightInfo.getDescriptor(),
                                List.of(e), flightInfo.getBytes(),
                                flightInfo.getRecords()))
                .toArray(FlightInfo[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new ArrowPartitionReaderFactory(outputSchema, requiredPartitionSchema, requiredPartitions, datasourceOptions);
    }

    @Override
    public StructType readSchema() {
        return outputSchema;
    }

    @Override
    public String description() {
        return String.format("ArrowRPCScan[endpoints=%d, columns=%d]",
                flightInfo.getEndpoints().size(), outputSchema.fields().length);
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    public boolean hasPushedAggregation() {
        return pushedAggregation;
    }
}
