package io.github.sullis.duckdb.playground;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.duckdb.DuckDBConnection;

public class Duckdb {
  public static final String DRIVER_CLASS_NAME = "org.duckdb.DuckDBDriver";

  public static void loadDriver() throws ClassNotFoundException {
    Class.forName(DRIVER_CLASS_NAME);
  }

  static {
    try {
      loadDriver();
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("unable to load DuckDB driver", ex);
    }
  }

  private Duckdb() { }

  static public void initializeExtensions() throws SQLException {
    try (DuckDBConnection conn = getConnection(false)) {
      try (Statement statement = conn.createStatement()) {
        // https://duckdb.org/docs/extensions/iceberg
        statement.addBatch("install iceberg;");
        statement.addBatch("load iceberg;");

        // https://duckdb.org/docs/extensions/httpfs/overview.html
        statement.addBatch("install httpfs;");
        statement.addBatch("load httpfs;");

        // https://duckdb.org/docs/extensions/json
        statement.addBatch("install json;");
        statement.addBatch("load json;");

        statement.executeBatch();
      }
    }
  }

  public static DuckDBConnection getConnection(boolean readOnly)
      throws SQLException {
    Properties props = new Properties();
    if (readOnly) {
      props.setProperty("duckdb.read_only", "true");
    }
    return (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", props);
  }
}
