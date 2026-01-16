package io.dazzleduck.sql.spark;

import org.apache.arrow.driver.jdbc.ArrowFlightConnection;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum FlightSqlClientPool implements Closeable {
    INSTANCE;
    private static final Duration CLOSE_DELAY = Duration.ofMinutes(2);
    private static final Logger logger = LoggerFactory.getLogger(FlightSqlClientPool.class);
    private final Map<String, ClientAndCreationTime> cache = new ConcurrentHashMap<>();
    private final Map<String, HttpClientInfo> httpCache = new ConcurrentHashMap<>();
    private final RootAllocator allocator = new RootAllocator();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private final java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
            .connectTimeout(Duration.ofMinutes(2))
            .build();

    FlightSqlClientPool() {
    }

    private static boolean isHttpProtocol(String url) {
        return url != null && (url.startsWith("http://") || url.startsWith("https://"));
    }

    private static FlightSqlClient getPrivateClientFromHandler(ArrowFlightSqlClientHandler handler) {
        try {
            Field clientField = ArrowFlightSqlClientHandler.class.getDeclaredField("sqlClient");
            clientField.setAccessible(true);
            return (FlightSqlClient) clientField.get(handler);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ArrowFlightSqlClientHandler getPrivateClientHandlerFromConnection(ArrowFlightConnection connection) {
        try {
            Field clientHandler = ArrowFlightConnection.class.getDeclaredField("clientHandler");
            clientHandler.setAccessible(true);
            return (ArrowFlightSqlClientHandler) clientHandler.get(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static CallOption[] getPrivateCallOptions(ArrowFlightSqlClientHandler arrowFlightSqlClientHandler) {
        try {
            Method callOptions = ArrowFlightSqlClientHandler.class.getDeclaredMethod("getOptions");
            callOptions.setAccessible(true);
            return (CallOption[]) callOptions.invoke(arrowFlightSqlClientHandler);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private String getKey(DatasourceOptions options) {
        return options.url() + options.getSourceOptions()
                .entrySet()
                .stream()
                .sorted()
                .map(e -> e.getKey() + ":" + e.getValue())
                .collect(Collectors.joining(","));
    }

    private ClientAndCreationTime getClient(DatasourceOptions options) {
        var mapKey = getKey(options);
        var timeout = options.connectionTimeout();
        return cache.compute(mapKey, (key, oldValue) -> {
            if (oldValue == null || oldValue.timestamp < System.currentTimeMillis() - timeout.toMillis()) {
                if (oldValue != null) {
                    scheduledExecutorService.schedule(() -> closeClient(oldValue.flightClient), CLOSE_DELAY.toMillis(), TimeUnit.MILLISECONDS);
                }
                try {
                    var connection = new ArrowFlightJdbcDriver().connect(options.url(), options.properties());
                    var ch = getPrivateClientHandlerFromConnection(connection);
                    var sqlClient = getPrivateClientFromHandler(ch);
                    var coptions = getPrivateCallOptions(ch);
                    return new ClientAndCreationTime(System.currentTimeMillis(), sqlClient, coptions);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                return oldValue;
            }
        });
    }

    private HttpClientInfo getHttpClient(DatasourceOptions options) {
        var mapKey = getKey(options);
        var timeout = options.connectionTimeout();
        return httpCache.compute(mapKey, (key, oldValue) -> {
            if (oldValue == null || oldValue.timestamp < System.currentTimeMillis() - timeout.toMillis()) {
                String token = loginAndGetToken(options);
                return new HttpClientInfo(
                        System.currentTimeMillis(),
                        options.url(),
                        options.properties(),
                        token
                );
            } else {
                return oldValue;
            }
        });
    }

    private String loginAndGetToken(DatasourceOptions options) {
        try {
            String username = options.properties().getProperty("username");
            String password = options.properties().getProperty("password");

            Properties p = options.properties();

            String claimsJson = Stream.of("schema", "database", "table")
                    .map(k -> Map.entry(k, p.getProperty(k)))
                    .filter(e -> e.getValue() != null && !e.getValue().isBlank())
                    .map(e -> String.format("\"%s\":\"%s\"", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(","));

            String requestBody = String.format(
                    "{\"username\":\"%s\",\"password\":\"%s\",\"claims\":{%s}}",
                    username, password, claimsJson
            );

            var loginRequest = java.net.http.HttpRequest.newBuilder()
                    .uri(URI.create(options.url() + "/v1/login"))
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(requestBody))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .timeout(Duration.ofSeconds(30))
                    .build();

            var response = httpClient.send(loginRequest, java.net.http.HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("Login failed with status: " + response.statusCode() +
                        ", body: " + response.body());
            }

            String responseBody = response.body();
            String accessToken = extractJsonField(responseBody, "accessToken");
            String tokenType = extractJsonField(responseBody, "tokenType");

            if (accessToken == null || accessToken.isEmpty()) {
                throw new RuntimeException("No access token in login response: " + responseBody);
            }

            return tokenType + " " + accessToken;

        } catch (Exception e) {
            logger.error("Error during HTTP login", e);
            throw new RuntimeException("Failed to login to HTTP server", e);
        }
    }

    private String extractJsonField(String json, String fieldName) {
        String pattern = "\"" + fieldName + "\":\"";
        int start = json.indexOf(pattern);
        if (start == -1) return null;
        start += pattern.length();
        int end = json.indexOf("\"", start);
        if (end == -1) return null;
        return json.substring(start, end);
    }

    public FlightInfo getInfo(DatasourceOptions options, String query, CallOption... callOptions) {
        if (isHttpProtocol(options.url())) {
            return getInfoViaHttp(options, query);
        } else {
            return getInfoViaGrpc(options, query, callOptions);
        }
    }

    private FlightInfo getInfoViaGrpc(DatasourceOptions options, String query, CallOption... callOptions) {
        var client = getClient(options);
        return client.flightClient.execute(query,
                Stream.concat(Arrays.stream(client.callOptions),
                        Arrays.stream(callOptions)).toArray(CallOption[]::new));
    }

    /**
     * For HTTP, execute the query to validate it works, then create a synthetic FlightInfo.
     * Note: The actual data will be fetched again in getStreamViaHttp.
     */
    private FlightInfo getInfoViaHttp(DatasourceOptions options, String query) {
        try {
            var client = getHttpClient(options);

            // Prepare request body - escape special characters
            String escapedQuery = query.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
            String requestBody = String.format("{\"query\":\"%s\"}", escapedQuery);

            logger.info("Validating HTTP query: {}", query);

            // Make a test request to verify the query works
            var requestBuilder = java.net.http.HttpRequest.newBuilder()
                    .uri(URI.create(client.baseUrl + "/v1/query"))
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(requestBody))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("Authorization", client.jwtToken)
                    .timeout(Duration.ofMinutes(5));

            var request = requestBuilder.build();
            var response = httpClient.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

            logger.info("HTTP query validation response: status={}", response.statusCode());

            if (response.statusCode() != 200) {
                String errorMsg = String.format("HTTP query validation failed: %d, body: %s",
                        response.statusCode(), response.body());
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }

            // Create a synthetic ticket containing JUST the query string
            Ticket ticket = new Ticket(query.getBytes(StandardCharsets.UTF_8));

            // Create a dummy location
            URI uri = URI.create(options.url());
            String host = uri.getHost() != null ? uri.getHost() : "localhost";
            int port = uri.getPort() > 0 ? uri.getPort() : 80;
            Location location = Location.forGrpcInsecure(host, port);

            // Create endpoint
            FlightEndpoint endpoint = new FlightEndpoint(ticket, location);

            // Create FlightInfo
            FlightDescriptor descriptor = FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8));

            return new FlightInfo(
                    new Schema(Collections.emptyList()),
                    descriptor,
                    Collections.singletonList(endpoint),
                    -1,
                    -1
            );

        } catch (Exception e) {
            logger.error("Error getting FlightInfo via HTTP", e);
            throw new RuntimeException(e);
        }
    }

    public FlightStream getStream(DatasourceOptions options, FlightEndpoint endpoint, CallOption... callOptions) throws java.sql.SQLException {
        if (isHttpProtocol(options.url())) {
            return getStreamViaHttp(options, endpoint);
        } else {
            return getStreamViaGrpc(options, endpoint, callOptions);
        }
    }

    private FlightStream getStreamViaGrpc(DatasourceOptions options, FlightEndpoint endpoint, CallOption... callOptions) throws java.sql.SQLException {
        var client = getClient(options);
        return client.flightClient.getStream(endpoint.getTicket(),
                Stream.concat(Arrays.stream(client.callOptions),
                        Arrays.stream(callOptions)).toArray(CallOption[]::new));
    }

    private FlightStream getStreamViaHttp(DatasourceOptions options, FlightEndpoint endpoint) {
        try {
            var client = getHttpClient(options);

            // Extract query from ticket
            String query = new String(endpoint.getTicket().getBytes(), StandardCharsets.UTF_8);
            String escapedQuery = query.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
            String requestBody = String.format("{\"query\":\"%s\"}", escapedQuery);

            logger.debug("Executing HTTP query: {}", query);

            var requestBuilder = java.net.http.HttpRequest.newBuilder()
                    .uri(URI.create(client.baseUrl + "/v1/query"))
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(requestBody))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/vnd.apache.arrow.stream")
                    .header("Authorization", client.jwtToken)
                    .timeout(Duration.ofMinutes(30));

            var request = requestBuilder.build();
            var response = httpClient.send(request, java.net.http.HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() != 200) {
                String errorMsg = String.format("HTTP query failed: %d", response.statusCode());
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }

            // Wrap HTTP Arrow stream
            var reader = new ArrowStreamReader(response.body(), allocator);
            return new HttpFlightStreamWrapper(reader, endpoint, allocator);

        } catch (Exception e) {
            logger.error("Error getting stream via HTTP", e);
            throw new RuntimeException(e);
        }
    }

    private void closeClient(FlightSqlClient flightClient) {
        try {
            flightClient.close();
        } catch (Exception e) {
            logger.atError().setCause(e).log("ERROR closing client" + flightClient);
        }
    }

    @Override
    public synchronized void close() {
        var it = cache.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            ClientAndCreationTime v = entry.getValue();
            try {
                v.flightClient().close();
            } catch (Exception e) {
                logger.atError().setCause(e).log("Error closing the connection : " + v);
            }
            it.remove();
        }

        httpCache.clear();
        allocator.close();
        scheduledExecutorService.shutdown();
    }

    record ClientAndCreationTime(long timestamp, FlightSqlClient flightClient, CallOption[] callOptions) {
    }

    record HttpClientInfo(long timestamp, String baseUrl, java.util.Properties properties, String jwtToken) {
    }
}