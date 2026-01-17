package io.dazzleduck.sql.spark;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ;

public class ArrowRPCTable implements Table, SupportsRead {

    private final StructType tableSchema;
    private final Identifier identifier;
    private final DatasourceOptions datasourceOptions;

    public ArrowRPCTable(StructType schema, Identifier identifier, DatasourceOptions datasourceOptions) {
        this.tableSchema = Objects.requireNonNull(schema, "schema cannot be null");
        this.identifier = Objects.requireNonNull(identifier, "identifier cannot be null");
        this.datasourceOptions = Objects.requireNonNull(datasourceOptions, "datasourceOptions cannot be null");
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new ArrowRPCScanBuilder(tableSchema, datasourceOptions);
    }

    @Override
    public String name() {
        return identifier.toString();
    }

    @Override
    public StructType schema() {
        return tableSchema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return EnumSet.of(BATCH_READ);
    }
}
