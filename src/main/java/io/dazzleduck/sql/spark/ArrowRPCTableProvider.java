package io.dazzleduck.sql.spark;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class ArrowRPCTableProvider implements TableProvider, DataSourceRegister {

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        throw new RuntimeException("Cannot infer schema");
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        return TableProvider.super.inferPartitioning(options);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        var datasourceOptions = DatasourceOptions.parse(properties);
        return new ArrowRPCTable(schema, getIdentifier(datasourceOptions), datasourceOptions);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public String shortName() {
        return "arrow-kafka";
    }

    public Identifier getIdentifier(DatasourceOptions opts) {
        // 1. Explicit identifier (CLI override)
        if (opts.identifier() != null && !opts.identifier().isBlank()) {
            return Identifier.of(new String[0], opts.identifier());
        }

        // 2. Path-based sources
        if (opts.path() != null && !opts.path().isBlank()) {
            return Identifier.of(new String[0], opts.path());
        }

        // 3. DuckLake auto-derivation
        if (opts.catalog() != null && opts.schema() != null && opts.table() != null) {
            String derivedIdentifier = opts.catalog() + "." + opts.schema() + "." + opts.table();
            return Identifier.of(new String[0], derivedIdentifier);
        }

        // 4. Fail fast with detailed error
        String errorMsg = String.format(
                "Cannot determine identifier. Provide identifier, path, or DuckLake database/schema/table. " +
                        "Received: identifier=%s, path=%s, catalog=%s, schema=%s, table=%s",
                opts.identifier(), opts.path(), opts.catalog(), opts.schema(), opts.table()
        );

        throw new IllegalArgumentException(errorMsg);
    }
}