package io.dazzleduck.sql.spark;

import java.io.Serializable;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.*;

public record DatasourceOptions(
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
        String table
) implements Serializable {

    public static final String CONNECTION_TIMEOUT = "connection_timeout";
    public static final String IDENTIFIER_KEY = "identifier";
    public static final String PATH_KEY = "path";
    public static final String URL_KEY = "url";
    public static final String FUNCTION_KEY = "function";
    public static final String PARTITION_COLUMNS_KEY = "partition_columns";

    // Note: "database" key maps to catalog field for backwards compatibility
    public static final String CATALOG_KEY = "database";
    public static final String SCHEMA_KEY = "schema";
    public static final String TABLE_KEY = "table";

    public static final Set<String> EXCLUDE_PROPS = Set.of(
            IDENTIFIER_KEY, URL_KEY, PARTITION_COLUMNS_KEY, CONNECTION_TIMEOUT
    );

    public enum SourceType {HIVE, DUCKLAKE}

    public DatasourceOptions {
        // Defensive copy of properties
        if (properties != null) {
            Properties copy = new Properties();
            copy.putAll(properties);
            properties = copy;
        }
    }

    public static DatasourceOptions parse(Map<String, String> properties) {
        var url = properties.get(URL_KEY);
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("%s is required".formatted(URL_KEY));
        }

        var identifier = properties.get(IDENTIFIER_KEY);
        var path = properties.get(PATH_KEY);
        var function = properties.get(FUNCTION_KEY);
        var catalog = properties.get(CATALOG_KEY);
        var schema = properties.get(SCHEMA_KEY);
        var table = properties.get(TABLE_KEY);

        var partitionColumnString = properties.get(PARTITION_COLUMNS_KEY);
        var timeoutString = properties.get(CONNECTION_TIMEOUT);
        if (timeoutString == null) {
            throw new IllegalArgumentException("%s is required".formatted(CONNECTION_TIMEOUT));
        }

        Duration timeout;
        try {
            timeout = Duration.parse(timeoutString);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "Unable to parse %s value '%s'. Expected ISO-8601 duration format PnDTnHnMn.nS (e.g., PT10M for 10 minutes)"
                            .formatted(CONNECTION_TIMEOUT, timeoutString), e);
        }

        List<String> partitionColumns = partitionColumnString == null
                ? List.of()
                : Arrays.stream(partitionColumnString.split(",")).map(String::trim).toList();

        SourceType sourceType = (catalog != null && schema != null && table != null)
                ? SourceType.DUCKLAKE
                : SourceType.HIVE;

        Properties propsWithout = new Properties();
        properties.forEach((key, value) -> {
            if (!EXCLUDE_PROPS.contains(key)) {
                propsWithout.put(key, value);
            }
        });

        return new DatasourceOptions(
                url, identifier, path, function, partitionColumns, timeout,
                propsWithout, sourceType, catalog, schema, table
        );
    }

    public Map<String, String> getSourceOptions() {
        if (path != null) {
            return Map.of(PATH_KEY, PathUtil.toDazzleDuckPath(path));
        }
        if (identifier != null) {
            return Map.of(IDENTIFIER_KEY, identifier);
        }
        if (catalog != null && schema != null && table != null) {
            // Quote each part to handle identifiers containing dots
            String derivedIdentifier = "\"%s\".\"%s\".\"%s\"".formatted(
                    escapeIdentifier(catalog),
                    escapeIdentifier(schema),
                    escapeIdentifier(table)
            );
            return Map.of(IDENTIFIER_KEY, derivedIdentifier);
        }
        return Map.of();
    }

    private static String escapeIdentifier(String identifier) {
        // Escape double quotes by doubling them
        return identifier.replace("\"", "\"\"");
    }
}