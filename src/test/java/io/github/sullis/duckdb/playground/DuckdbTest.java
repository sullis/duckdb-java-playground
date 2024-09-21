package io.github.sullis.duckdb.playground;

import java.sql.DriverManager;
import java.util.Set;
import java.util.stream.Collectors;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DuckdbTest {
  @BeforeAll
  public static void loadDriver() throws Exception {
    Duckdb.loadDriver();
    Set<String> drivers = DriverManager.drivers()
        .map(d -> d.getClass().getName())
        .collect(Collectors.toSet());
    assertThat(drivers).contains(Duckdb.DRIVER_CLASS_NAME);
  }

  @Test
  public void happyPath() throws Exception {
    Duckdb.initializeIceberg();
    try (DuckDBConnection conn = Duckdb.getConnection()) {
      assertThat(conn).isNotNull();
      assertThat(conn.isClosed()).isFalse();
    }
  }
}
