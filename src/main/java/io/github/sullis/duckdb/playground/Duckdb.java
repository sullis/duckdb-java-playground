package io.github.sullis.duckdb.playground;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.duckdb.DuckDBConnection;

public class Duckdb {
  public static final String DRIVER_CLASS_NAME = "org.duckdb.DuckDBDriver";

  public Duckdb() { }

  public static void loadDriver() throws Exception {
     Class.forName(DRIVER_CLASS_NAME);
  }

  public void initializeIceberg() throws SQLException {
    try (DuckDBConnection conn = getConnection()) {
      try (Statement statement = conn.createStatement()) {
        // https://duckdb.org/docs/extensions/iceberg
        statement.addBatch("install iceberg;");
        statement.addBatch("load iceberg;");

        // https://duckdb.org/docs/extensions/httpfs/overview.html
        statement.addBatch("install httpfs;");
        statement.addBatch("load httpfs;");

        statement.executeBatch();
      }
    }
  }

  public DuckDBConnection getConnection()
      throws SQLException {
    return (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
  }
}
