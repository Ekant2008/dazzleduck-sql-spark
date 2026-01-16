package io.dazzleduck.sql.spark;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

public record DatasourceOptions (
        String url,
        String identifier,
        String path,
        String function,
        List<String> partitionColumns,
        Duration connectionTimeout,
        Properties properties,
        SourceType sourceType,
        String catalog,
        String schema,
        String table,
        Protocol protocol
) implements Serializable {

    public static final String CONNECTION_TIMEOUT = "connection_timeout";
    public static final String IDENTIFIER_KEY = "identifier";
    public static final String PATH_KEY = "path";
    public static final String URL_KEY = "url";
    public static final String FUNCTION_KEY = "function";
    public static final String PARTITION_COLUMNS_KEY = "partition_columns";
    public static final String PROTOCOL_KEY = "protocol";

    public static final String DATABASE_KEY = "database";
    public static final String SCHEMA_KEY  = "schema";
    public static final String TABLE_KEY   = "table";

    public static final Set<String> EXCLUDE_PROPS = Set.of(
            IDENTIFIER_KEY, URL_KEY, PARTITION_COLUMNS_KEY, CONNECTION_TIMEOUT, PROTOCOL_KEY);

    public enum SourceType {HIVE, DUCKLAKE}

    public enum Protocol {
        GRPC,    // jdbc:arrow-flight-sql://
        HTTP,    // http:// or https://
        AUTO     // Auto-detect from URL
    }

    public static DatasourceOptions parse(Map<String, String> properties) {

        var url        = properties.get(URL_KEY);
        var identifier = properties.get(IDENTIFIER_KEY);
        var path       = properties.get(PATH_KEY);
        var function   = properties.get(FUNCTION_KEY);
        var catalog = properties.get(DATABASE_KEY);
        var schema  = properties.get(SCHEMA_KEY);
        var table   = properties.get(TABLE_KEY);

        var partitionColumnString = properties.get(PARTITION_COLUMNS_KEY);
        var timeoutString = properties.get(CONNECTION_TIMEOUT);
        var protocolString = properties.get(PROTOCOL_KEY);

        if(timeoutString == null) {
            throw new RuntimeException("%s value is required".formatted(CONNECTION_TIMEOUT));
        }

        Duration timeout;
        try {
            timeout = Duration.parse(timeoutString);
        } catch (Exception e) {
            throw new RuntimeException("Unable to parse timeout value %s. The formats accepted are based on the ISO-8601 duration format PnDTnHnMn.nS with days considered to be exactly 24 hours".formatted(timeoutString));
        }

        List<String> partitionColumns = partitionColumnString == null
                ? List.of()
                : Arrays.stream(partitionColumnString.split(","))
                .map(String::trim)
                .toList();

        SourceType sourceType = (catalog != null && schema != null && table != null)
                ? SourceType.DUCKLAKE
                : SourceType.HIVE;

        // Determine protocol
        Protocol protocol = determineProtocol(url, protocolString);

        Properties propsWithout = new Properties();
        properties.forEach((key, value) -> {
            if (!EXCLUDE_PROPS.contains(key)) {
                propsWithout.put(key, value);
            }
        });

        return new DatasourceOptions(
                url, identifier, path, function, partitionColumns,
                timeout, propsWithout, sourceType, catalog, schema, table, protocol);
    }

    private static Protocol determineProtocol(String url, String protocolString) {
        // Explicit protocol specified
        if (protocolString != null && !protocolString.isBlank()) {
            try {
                return Protocol.valueOf(protocolString.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Invalid protocol: " + protocolString +
                        ". Valid values are: GRPC, HTTP, AUTO");
            }
        }

        // Auto-detect from URL
        if (url == null || url.isBlank()) {
            return Protocol.GRPC; // Default
        }

        if (url.startsWith("http://") || url.startsWith("https://")) {
            return Protocol.HTTP;
        } else if (url.startsWith("jdbc:arrow-flight-sql://") || url.startsWith("grpc://")) {
            return Protocol.GRPC;
        }

        return Protocol.GRPC; // Default fallback
    }

    public boolean isHttpProtocol() {
        return protocol == Protocol.HTTP;
    }

    public boolean isGrpcProtocol() {
        return protocol == Protocol.GRPC;
    }

    public Map<String, String> getSourceOptions() {
        if (path != null) {
            return Map.of(PATH_KEY, PathUtil.toDazzleDuckPath(path));
        }
        if (identifier != null) {
            return Map.of(IDENTIFIER_KEY, identifier);
        }
        if (catalog != null && schema != null && table != null) {
            String derivedIdentifier = catalog + "." + schema + "." + table;
            return Map.of(IDENTIFIER_KEY, derivedIdentifier);
        }
        return Map.of();
    }
}