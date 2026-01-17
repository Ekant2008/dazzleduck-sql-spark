package io.dazzleduck.sql.spark;

import io.dazzleduck.sql.flight.server.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlightTestUtil {

    private static final Logger logger = LoggerFactory.getLogger(FlightTestUtil.class);
    private static final long SERVICE_STARTUP_DELAY_MS = 2000;

    public static void createFlightServiceAndStart(int port) throws Exception {
        startFlightService(port, "RESTRICTED");
    }

    public static void createFlightServiceAndStartComplete(int port) throws Exception {
        startFlightService(port, "COMPLETE");
    }

    private static void startFlightService(int port, String accessMode) throws Exception {
        String[] args = {
                "--conf", "dazzleduck_server.flight_sql.port=" + port,
                "--conf", "dazzleduck_server.flight_sql.use_encryption=false",
                "--conf", "dazzleduck_server.access_mode=" + accessMode
        };
        Main.main(args);
        logger.info("Flight service started on port {} with access mode {}", port, accessMode);
        Thread.sleep(SERVICE_STARTUP_DELAY_MS);
    }
}
