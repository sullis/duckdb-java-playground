package io.github.sullis.duckdb.playground;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
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

  public void initializeExtensions(List<String> extensions)
      throws SQLException {
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

  public List<String> listExtensions()
      throws SQLException {
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

  public List<String> listTables()
      throws SQLException {
    List<String> result = new ArrayList<>();
    try (DuckDBConnection conn = getConnection()) {
      try (ResultSet rs = conn.getMetaData().getTables(null, null, null, null)) {
        while (rs.next()) {
          result.add(rs.getString("TABLE_NAME"));
        }
      }
    }
    return result;
  }

  public DuckDBConnection getConnection()
      throws SQLException {
    return (DuckDBConnection) DriverManager.getConnection(jdbcUrl);
  }

  public void createTable(String tableName, Map<String, String> columns, List<String> primaryKeys)
      throws SQLException {

    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE ");
    sql.append(tableName);
    sql.append(" (");

    StringJoiner columnJoiner = new StringJoiner(",");
    for (Map.Entry column : columns.entrySet()) {
      columnJoiner.add(column.getKey() + " " + column.getValue());
    }
    sql.append(columnJoiner.toString());

    if (primaryKeys != null) {
      sql.append(" , PRIMARY KEY(" + String.join(",", primaryKeys) + ") ");
    }
    ;
    sql.append(") ");
    sql.append(";");
    executeUpdate(sql);
  }

  public int countRows(String tableName)
      throws SQLException {
    try (DuckDBConnection conn = getConnection()) {
      try (Statement statement = conn.createStatement()) {
        try (ResultSet rs = statement.executeQuery("select count(*) from " + tableName + ";")) {
          rs.next();
          return rs.getInt(1);
        }
      }
    }
  }

  public void executeUpdate(CharSequence sql) throws SQLException {
    try (DuckDBConnection conn = getConnection()) {
      conn.setAutoCommit(true);
      try (Statement statement = conn.createStatement()) {
        statement.executeUpdate(sql.toString());
      }
    }
  }

  public void insertInto(String tableName, Map<String, Object> rowData)
      throws SQLException {
    List<String> columnNames = new ArrayList<>(rowData.keySet());
    StringBuilder sql = new StringBuilder();
    sql.append("insert into ");
    sql.append(tableName);
    sql.append(" ( ");
    StringJoiner colJoiner = new StringJoiner(",");
    columnNames.forEach(colJoiner::add);
    sql.append(colJoiner);
    sql.append(" ) values ( ");
    StringJoiner valJoiner = new StringJoiner(",");
    columnNames.forEach(c -> valJoiner.add("?"));
    sql.append(valJoiner);
    sql.append(" ); ");

    try (DuckDBConnection conn = getConnection()) {
      conn.setAutoCommit(true);
      try (PreparedStatement statement = conn.prepareStatement(sql.toString())) {
        int paramIndex = 0;
        for (String colName : columnNames) {
          paramIndex++;
          Object colValue = rowData.get(colName);
          statement.setObject(paramIndex, colValue);
        }
        statement.execute();
      }
    }
  }

  public List<Map<String, Object>> query(String sql) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (DuckDBConnection conn = getConnection()) {
      try (Statement statement = conn.createStatement()) {
        try (ResultSet rs = statement.executeQuery(sql.toString())) {
          int columnCount = rs.getMetaData().getColumnCount();
          while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
             for (int i = 0; i < columnCount; i++) {
               String colName = rs.getMetaData().getColumnName(i+1);
               row.put(colName.toString(), rs.getObject(colName));
             }
            rows.add(row);
          }
        }
      }
    }
    return rows;
  }

  public org.duckdb.JsonNode queryForJsonNode(CharSequence sql) throws SQLException {
    return queryForSingleValue(sql, org.duckdb.JsonNode.class);
  }

  public String queryForJsonString(CharSequence sql) throws SQLException {
    var node = queryForJsonNode(sql);
    if (!node.isString()) {
      throw new SQLException("wrong type of JsonNode.  Expected a string");
    }
    String s = node.toString();
    if (s.isEmpty()) {
      return s;
    } else {
      return s.substring(1, s.length() - 1);
    }
  }

  public <T> T queryForSingleValue(CharSequence sql, Class<T> clazz) throws SQLException {
    try (DuckDBConnection conn = getConnection()) {
      try (Statement statement = conn.createStatement()) {
        try (ResultSet rs = statement.executeQuery(sql.toString())) {
          if (rs.next()) {
            return (T) rs.getObject(1);
          } else {
            return (T) null;
          }
        }
      }
    }
  }
}
