package io.github.sullis.duckdb.playground;

import java.sql.DriverManager;
import java.util.Set;
import java.util.stream.Collectors;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class DuckdbDriverTest {
  private static final String DRIVER_CLASS_NAME = "org.duckdb.DuckDBDriver";

  @BeforeAll
  public static void loadDriver() throws Exception {
    Class.forName(DRIVER_CLASS_NAME);
    Set<String> drivers = DriverManager.drivers()
        .map(d -> d.getClass().getName())
        .collect(Collectors.toSet());
    assertThat(drivers).contains(DRIVER_CLASS_NAME);
  }

  @Test
  public void happyPath() throws Exception {
    DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
    assertThat(conn).isNotNull();
    assertThat(conn.isClosed()).isFalse();
  }
}
