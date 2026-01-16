package io.dazzleduck.sql.spark;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

/**
 * Wrapper class that adapts an ArrowStreamReader (from HTTP response) to FlightStream interface
 */
public class HttpFlightStreamWrapper extends FlightStream {

    private final ArrowStreamReader reader;
    private final FlightEndpoint endpoint;

    public HttpFlightStreamWrapper(ArrowStreamReader reader, FlightEndpoint endpoint, org.apache.arrow.memory.BufferAllocator allocator) throws IOException {
        // Pass the allocator provided during reader creation
        super(allocator,
                0,
                (stream, root) -> {},
                null);
        this.reader = reader;
        this.endpoint = endpoint;
    }

    @Override
    public Schema getSchema() {
        try {
            // ArrowStreamReader provides the schema via the VectorSchemaRoot
            return reader.getVectorSchemaRoot().getSchema();
        } catch (IOException e) {
            throw new RuntimeException("Failed to retrieve schema", e);
        }
    }

    @Override
    public VectorSchemaRoot getRoot() {
        try {
            return reader.getVectorSchemaRoot();
        } catch (IOException e) {
            throw new RuntimeException("Failed to retrieve root", e);
        }
    }

    @Override
    public boolean next() {
        try {
            // reader.loadNextBatch() returns true if a batch was loaded, false at EOF
            return reader.loadNextBatch();
        } catch (IOException e) {
            throw new RuntimeException("Error reading next batch from HTTP stream", e);
        }
    }

    @Override
    public FlightDescriptor getDescriptor() {
        if (endpoint != null && endpoint.getLocations() != null && !endpoint.getLocations().isEmpty()) {
            return FlightDescriptor.path(endpoint.getLocations().get(0).getUri().getPath());
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    public long bytesRead() {
        return reader.bytesRead();
    }
}