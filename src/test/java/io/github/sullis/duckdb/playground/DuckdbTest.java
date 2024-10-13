package io.github.sullis.duckdb.playground;

import java.io.File;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DuckdbTest {
  public static final List<String> EXTENSIONS = List.of(
      // https://duckdb.org/docs/extensions/iceberg
      "iceberg",
      // https://duckdb.org/docs/extensions/httpfs/overview.html
      "httpfs",
      // https://duckdb.org/docs/extensions/aws
      "aws",
      // https://duckdb.org/docs/extensions/json
      "json"
  );

  private static final String JSON1 = """
  {
      "a1": "a1-value",
      "a2": "a2-value"
  }""";

  private static final String JSON2 = """
  {
      "b1": "b1-value",
      "b2": "b2-value"
  }""";

  private static final String JSON3 = """
  {
      "c1": "c1-value",
      "c2": "c2-value"
  }""";

  private static final String dbfile = "/tmp/testdb-" + System.currentTimeMillis();
  private static Duckdb duckdb;

  @BeforeAll
  public static void setupDuckDb() throws Exception {
    File f = new File(dbfile);
    f.deleteOnExit();
    duckdb = new Duckdb("jdbc:duckdb:" + dbfile);
    duckdb.initializeExtensions(EXTENSIONS);
    Set<String> drivers = DriverManager.drivers()
        .map(d -> d.getClass().getName())
        .collect(Collectors.toSet());
    assertThat(drivers).contains(Duckdb.DRIVER_CLASS_NAME);
  }

  @Test
  public void happyPath() throws Exception {
    assertThat(duckdb.listExtensions())
        .contains("iceberg", "json", "httpfs", "aws");

    try (DuckDBConnection conn = duckdb.getConnection()) {
      assertThat(conn).isNotNull();
      assertThat(conn.isReadOnly()).isFalse();
      assertThat(conn.isClosed()).isFalse();
    }

    final String tableName = "things";

    duckdb.createTable(tableName,
        Map.of("id", "varchar",
              "a", "json",
              "b", "json",
              "c", "json"),
        List.of("id"));

    assertThat(duckdb.listTables())
        .contains(tableName);

    assertThat(duckdb.countRows(tableName))
        .isEqualTo(0);

    final UUID rowId = UUID.randomUUID();

    duckdb.insertInto(tableName,
        Map.of("id", rowId.toString(),
          "a", JSON1,
          "b", JSON2,
          "c", JSON3));

    assertThat(duckdb.countRows(tableName))
        .isEqualTo(1);

    String singleValue = duckdb.queryForSingleValue(
        "select id from " + tableName,
        String.class);
    assertThat(singleValue).isEqualTo(rowId.toString());

    var aJsonValue = duckdb.queryForJsonNode("select a.a1 from " + tableName);
    assertThat(aJsonValue.isString()).isTrue();
    assertThat(aJsonValue.toString()).isEqualTo("\"a1-value\"");

    var bJsonValue = duckdb.queryForJsonNode("select b.b1 from " + tableName);
    assertThat(bJsonValue.isString()).isTrue();
    assertThat(bJsonValue.toString()).isEqualTo("\"b1-value\"");

    var cJsonValue = duckdb.queryForJsonNode("select c.c1 from " + tableName);
    assertThat(cJsonValue.isString()).isTrue();
    assertThat(cJsonValue.toString()).isEqualTo("\"c1-value\"");

    List<Map<String, Object>> rowData = duckdb.query("select id from " + tableName);
    assertThat(rowData).contains(
        Map.of("id", rowId.toString()));

  }
}
