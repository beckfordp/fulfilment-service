package fulfilmentservice.examples

import cats.effect.IO
import cats.syntax.all._
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.testcontainers.utility.DockerImageName
// Session.Builder needs a Tracer/Meter in scope for its own instrumentation
// - no-ops here, same as FulfilmentStore.postgres's own
// construction (this example doesn't exercise real tracing/metrics).
import org.typelevel.otel4s.metrics.Meter.Implicits.noop
import org.typelevel.otel4s.trace.Tracer.Implicits.noop
import skunk.Session
import skunk.codec.all._
import skunk.implicits._

import java.util.UUID

import fulfilmentservice.{Migrations, PostgresConfig}

/** This template's reference service writes to its base entity table only -
  * it has no Kafka event to publish, so there's no outbox write wired into
  * the real FulfilmentStore. When you adopt the outbox-CDC
  * pattern (ADR 0008 in gluon - write the Kafka-bound event in the same
  * Postgres transaction as the state change it announces, instead of a
  * separate, non-transactional Kafka publish call), insert into
  * `outbox_events` (see `V2__create_outbox_events.sql`) from inside the
  * same `session.transaction.use` block as your own state-change
  * statement, as shown here - against a self-contained session and the
  * base `fulfilment` table, rather than the real
  * `FulfilmentStore`. See `docs/examples.md`, and
  * order-service's `OrderStore.scala` (`insertOutboxRow`) for the fuller,
  * real version of this same pattern.
  */
class OutboxExampleSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(dockerImageName =
      DockerImageName.parse("postgres:16-alpine")
    )

  private def configFor(postgres: PostgreSQLContainer): PostgresConfig =
    PostgresConfig(
      host = postgres.host,
      port = postgres.mappedPort(5432),
      database = postgres.databaseName,
      user = postgres.username,
      password = postgres.password
    )

  private val insertEntity: skunk.Command[UUID] =
    sql"""INSERT INTO "fulfilment" (id) VALUES ($uuid)""".command

  private val insertOutboxEvent
      : skunk.Command[(UUID, String, String, String)] =
    sql"""
      INSERT INTO outbox_events (id, aggregate_id, payload, correlation_id)
      VALUES ($uuid, $text, $text::jsonb, $text)
    """.command

  private val countEntity: skunk.Query[UUID, Int] =
    sql"""SELECT count(*)::int4 FROM "fulfilment" WHERE id = $uuid"""
      .query(int4)

  /** Writes a `fulfilment` row and an `outbox_events` row in the same
    * Postgres transaction - the whole point of the pattern: the Kafka-bound
    * event becomes a property of that one transaction committing, not a
    * separate, non-transactional publish call. If either statement fails,
    * the whole transaction rolls back - neither row persists.
    */
  private def insertEntityAndOutboxEvent(
      session: Session[IO],
      entityId: UUID,
      outboxId: UUID,
      payload: String
  ): IO[Unit] =
    session.transaction.use { _ =>
      session.prepare(insertEntity).flatMap(_.execute(entityId)) *>
        session
          .prepare(insertOutboxEvent)
          .flatMap(
            _.execute(
              (outboxId, entityId.toString, payload, entityId.toString)
            )
          )
          .void
    }

  test(
    "writing the entity and its outbox event happen in the same transaction"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> Session
        .Builder[IO]
        .withHost(config.host)
        .withPort(config.port)
        .withUserAndPassword(config.user, config.password)
        .withDatabase(config.database)
        .pooled(max = 1)
        .use { pool =>
          pool.use { session =>
            val entityId = UUID.randomUUID()
            val outboxId = UUID.randomUUID()
            for {
              _ <- insertEntityAndOutboxEvent(
                session,
                entityId,
                outboxId,
                """{"example":true}"""
              )
              entityCount <- session
                .prepare(countEntity)
                .flatMap(_.unique(entityId))
            } yield assertEquals(entityCount, 1)
          }
        }
    }
  }

  test(
    "if the outbox write fails, the entity write in the same transaction is rolled back too"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> Session
        .Builder[IO]
        .withHost(config.host)
        .withPort(config.port)
        .withUserAndPassword(config.user, config.password)
        .withDatabase(config.database)
        .pooled(max = 1)
        .use { pool =>
          pool.use { session =>
            val sharedOutboxId = UUID.randomUUID()
            val firstEntityId = UUID.randomUUID()
            val secondEntityId = UUID.randomUUID()
            for {
              _ <- insertEntityAndOutboxEvent(
                session,
                firstEntityId,
                sharedOutboxId,
                """{"example":true}"""
              )
              // Reusing sharedOutboxId violates outbox_events' primary key,
              // failing the second transaction entirely.
              secondAttempt <- insertEntityAndOutboxEvent(
                session,
                secondEntityId,
                sharedOutboxId,
                """{"example":true}"""
              ).attempt
              secondEntityCount <- session
                .prepare(countEntity)
                .flatMap(_.unique(secondEntityId))
            } yield {
              assert(
                secondAttempt.isLeft,
                "expected the duplicate outbox id to fail the second transaction, got " + secondAttempt
              )
              assertEquals(secondEntityCount, 0)
            }
          }
        }
    }
  }
}
