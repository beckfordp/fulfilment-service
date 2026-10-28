package fulfilmentservice

import cats.effect.IO
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.testcontainers.utility.DockerImageName

import java.sql.DriverManager

class MigrationsSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(dockerImageName =
      DockerImageName.parse("postgres:16-alpine")
    )

  test("running migrations creates the fulfilment table") {
    withContainers { postgres =>
      val config = PostgresConfig(
        host = postgres.host,
        port = postgres.mappedPort(5432),
        database = postgres.databaseName,
        user = postgres.username,
        password = postgres.password
      )

      Migrations.run[IO](config).map { _ =>
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val rs = conn
            .createStatement()
            .executeQuery(
              "select column_name, data_type from information_schema.columns where table_name = 'fulfilment' order by ordinal_position"
            )
          val columns = Iterator
            .unfold(())(_ =>
              if (rs.next()) Some((rs.getString("column_name"), ())) else None
            )
            .toList
          assertEquals(
            columns,
            List(
              "id",
              // codegen:fields:TEST_MIGRATION_COLUMN
              "created_at",
              "updated_at"
            )
          )
        } finally conn.close()
      }
    }
  }

  // The outbox_events table backs OutboxExampleSuite
  // (src/test/scala/fulfilmentservice/examples/) - see docs/examples.md for what it
  // demonstrates and how to remove it if you don't adopt the outbox-CDC
  // pattern.
  test("running migrations creates the outbox_events table") {
    withContainers { postgres =>
      val config = PostgresConfig(
        host = postgres.host,
        port = postgres.mappedPort(5432),
        database = postgres.databaseName,
        user = postgres.username,
        password = postgres.password
      )

      Migrations.run[IO](config).map { _ =>
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val rs = conn
            .createStatement()
            .executeQuery(
              "select column_name from information_schema.columns where table_name = 'outbox_events' order by ordinal_position"
            )
          val columns = Iterator
            .unfold(())(_ =>
              if (rs.next()) Some((rs.getString("column_name"), ())) else None
            )
            .toList
          assertEquals(
            columns,
            List(
              "id",
              "aggregate_id",
              "payload",
              "trace_parent",
              "correlation_id",
              "created_at"
            )
          )
        } finally conn.close()
      }
    }
  }
}
