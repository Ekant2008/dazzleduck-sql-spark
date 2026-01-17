package io.dazzleduck.sql.spark;

public class PathUtil {

    private static final String S3A_SCHEME = "s3a://";
    private static final String S3_SCHEME = "s3://";
    private static final String FILE_SCHEME = "file:";

    private PathUtil() {
    }

    public static String toS3APath(String path) {
        if (path == null) {
            return null;
        }
        if (path.startsWith(S3_SCHEME) && !path.startsWith(S3A_SCHEME)) {
            return S3A_SCHEME + path.substring(S3_SCHEME.length());
        }
        return path;
    }

    public static String toS3Path(String path) {
        if (path == null) {
            return null;
        }
        if (path.startsWith(S3A_SCHEME)) {
            return S3_SCHEME + path.substring(S3A_SCHEME.length());
        }
        return path;
    }

    public static String toLocalPath(String path) {
        if (path == null) {
            return null;
        }
        if (path.startsWith(FILE_SCHEME)) {
            return path.substring(FILE_SCHEME.length());
        }
        return path;
    }

    public static String toDazzleDuckPath(String path) {
        return toLocalPath(toS3Path(path));
    }
}
