package io.dazzleduck.sql.spark;


import io.dazzleduck.sql.flight.server.Main;


public class FlightTestUtil {
    public static void createFsServiceAndStartHttp(int port) throws Exception {

        String[] args1 = {
                "--conf", "dazzleduck_server.http.port=" + port,
                "--conf", "dazzleduck_server.http.authentication=jwt",
                "--conf", "dazzleduck_server.access_mode=RESTRICTED"
        };
        io.dazzleduck.sql.http.server.Main.main(args1);
        System.out.println("Running service ");
        Thread.sleep(2000);
    }
    public static void createFsServiceAndStart(int port) throws Exception {
        String[] args = {
                "--conf", "dazzleduck_server.flight_sql.port=" + port,
                "--conf", "dazzleduck_server.flight_sql.use_encryption=false",
                "--conf", "dazzleduck_server.access_mode=RESTRICTED"
        };
        Main.main(args);
        System.out.println("Running service ");
        Thread.sleep(2000);
    }
    public static void createFsServiceAndStart2(int port) throws Exception {
        String[] args = {
                "--conf", "dazzleduck_server.flight_sql.port=" + port,
                "--conf", "dazzleduck_server.flight_sql.use_encryption=false",
                "--conf", "dazzleduck_server.access_mode=COMPLETE"
        };
        Main.main(args);
        System.out.println("Running service ");
        Thread.sleep(2000);
    }
}
