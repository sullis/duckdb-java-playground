package io.github.sullis.duckdb.playground;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.duckdb.DuckDBConnection;

public class Duckdb {
  public static final String DRIVER_CLASS_NAME = "org.duckdb.DuckDBDriver";

  static {
    try {
      Class.forName(DRIVER_CLASS_NAME);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("unable to load DuckDB driver", ex);
    }
  }

  private final String jdbcUrl;

  public Duckdb() {
    this("jdbc:duckdb:");
  }

  public Duckdb(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public void initializeExtensions(List<String> extensions) throws SQLException {
    try (DuckDBConnection conn = getConnection()) {
      try (Statement statement = conn.createStatement()) {
        for (String extension : extensions) {
          statement.addBatch("install " + extension + ";");
          statement.addBatch("load " + extension + ";");
        }
        statement.executeBatch();
      }
    }
  }

  public List<String> listExtensions() throws SQLException {
    List<String> result = new ArrayList<>();
    try (DuckDBConnection conn = getConnection()) {
      try (Statement statement = conn.createStatement()) {
        try (ResultSet rs = statement.executeQuery("SELECT extension_name FROM duckdb_extensions();")) {
          while (rs.next()) {
            result.add(rs.getString("extension_name"));
          }
        }
      }
    }
    return result;
  }

  public DuckDBConnection getConnection()
      throws SQLException {
    Properties props = new Properties();
    return (DuckDBConnection) DriverManager.getConnection(jdbcUrl, props);
  }
}
