package io.github.sullis.duckdb.playground;

import java.sql.DriverManager;
import java.util.List;
import java.util.Set;
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

  private static Duckdb duckdb;

  @BeforeAll
  public static void verifyJdbcDriver() throws Exception {
    duckdb = new Duckdb();
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
  }
}
