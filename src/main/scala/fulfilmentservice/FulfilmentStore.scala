package fulfilmentservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import skunk.Session
import skunk.codec.all._
import skunk.implicits._

import java.time.OffsetDateTime
import java.util.UUID
import scala.concurrent.duration.SECONDS

final case class Fulfilment(
    id: String,
    // codegen:fields:CASE_CLASS_FIELD
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

trait FulfilmentStore[F[_]] {
  def create( /* codegen:fields:CREATE_PARAMS */ ): F[Fulfilment]
  def get(id: String): F[Option[Fulfilment]]
  def update(
      id: String /* codegen:fields:UPDATE_PARAMS */
  ): F[Option[Fulfilment]]
  def delete(id: String): F[Boolean]
  def list(limit: Int, offset: Int): F[(List[Fulfilment], Long)]
  def ping: F[Boolean]
}

object FulfilmentStore {

  // codegen:fields:DEFAULT_VALUE_DECLS

  def inMemory[F[_]: Sync]: F[FulfilmentStore[F]] =
    Ref.of[F, Map[String, Fulfilment]](Map.empty).map { ref =>
      new FulfilmentStore[F] {
        def create( /* codegen:fields:CREATE_PARAMS */ ): F[Fulfilment] =
          for {
            id <- Sync[F].delay(java.util.UUID.randomUUID().toString)
            now <- Sync[F].realTimeInstant
            entity = Fulfilment(
              id, /* codegen:fields:CONSTRUCT_ARGS */ now,
              now
            )
            _ <- ref.update(_ + (id -> entity))
          } yield entity

        def get(id: String): F[Option[Fulfilment]] = ref.get.map(_.get(id))

        def update(
            id: String
            /* codegen:fields:UPDATE_PARAMS */
        ): F[Option[Fulfilment]] =
          for {
            now <- Sync[F].realTimeInstant
            updated <- ref.modify { entities =>
              entities.get(id) match {
                case None           => (entities, None)
                case Some(existing) =>
                  val next =
                    existing.copy(
                      /* codegen:fields:COPY_ARGS */
                      updatedAt = now
                    )
                  (entities + (id -> next), Some(next))
              }
            }
          } yield updated

        def delete(id: String): F[Boolean] =
          ref.modify { entities =>
            if (entities.contains(id)) (entities - id, true)
            else (entities, false)
          }

        def list(limit: Int, offset: Int): F[(List[Fulfilment], Long)] =
          ref.get.map { entities =>
            val sorted = entities.values.toList
              .sortBy(_.createdAt)(using Ordering[java.time.Instant].reverse)
            (sorted.slice(offset, offset + limit), sorted.size.toLong)
          }

        def ping: F[Boolean] = Sync[F].pure(true)
      }
    }

  private val insertFulfilment: skunk.Query[
    (UUID /* codegen:fields:SQL_INSERT_TUPLE_TYPE */ ),
    (OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      INSERT INTO "fulfilment" (id/* codegen:fields:SQL_INSERT_COLUMNS */)
      VALUES ($uuid/* codegen:fields:SQL_INSERT_PARAMS */)
      RETURNING created_at, updated_at
    """.query(timestamptz *: timestamptz)

  private val selectFulfilment: skunk.Query[
    UUID,
    ( /* codegen:fields:SQL_SELECT_TUPLE_TYPE */ OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      SELECT /* codegen:fields:SQL_SELECT_COLUMNS */created_at, updated_at
      FROM "fulfilment"
      WHERE id = $uuid
    """.query( /* codegen:fields:SQL_SELECT_CODEC */ timestamptz *: timestamptz)

  private val updateFulfilment: skunk.Query[
    ( /* codegen:fields:SQL_UPDATE_TUPLE_TYPE */ UUID),
    ( /* codegen:fields:SQL_SELECT_TUPLE_TYPE */ OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      UPDATE "fulfilment"
      SET /* codegen:fields:SQL_UPDATE_SET */updated_at = now()
      WHERE id = $uuid
      RETURNING /* codegen:fields:SQL_SELECT_COLUMNS */created_at, updated_at
    """.query( /* codegen:fields:SQL_SELECT_CODEC */ timestamptz *: timestamptz)

  private val deleteFulfilment: skunk.Query[UUID, UUID] =
    sql"""
      DELETE FROM "fulfilment"
      WHERE id = $uuid
      RETURNING id
    """.query(uuid)

  private val pingQuery: skunk.Query[skunk.Void, Int] = sql"SELECT 1".query(
    int4
  )

  // count(*) OVER() rides along with each row so the page and its total are
  // read from one consistent query result, not two separate round trips that
  // could otherwise observe different data if a write lands in between.
  private val selectFulfilmentPage: skunk.Query[
    (Int, Int),
    (
        UUID, /* codegen:fields:SQL_SELECT_TUPLE_TYPE */ OffsetDateTime,
        OffsetDateTime,
        Long
    )
  ] =
    sql"""
      SELECT id, /* codegen:fields:SQL_SELECT_COLUMNS */created_at, updated_at,
             count(*) OVER() AS total_count
      FROM "fulfilment"
      ORDER BY created_at DESC
      LIMIT $int4 OFFSET $int4
    """.query(
      uuid *: /* codegen:fields:SQL_SELECT_CODEC */ timestamptz *: timestamptz *: int8
    )

  // Fallback for when selectFulfilmentPage returns zero rows (empty
  // table, or offset past the end) -- count(*) OVER() has no row to ride along
  // with in that case, so the total still needs a direct query.
  private val countFulfilment: skunk.Query[skunk.Void, Long] =
    sql"""SELECT count(*) FROM "fulfilment"""".query(int8)

  def postgres[F[_]: Async: Console: Network](
      config: PostgresConfig,
      meter: Meter[F]
  ): Resource[F, FulfilmentStore[F]] = {
    import org.typelevel.otel4s.trace.Tracer.Implicits.noop
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .pooled(max = 10)
      .evalMap { pool =>
        meter
          .histogram[Double]("db.client.operation.duration")
          .withUnit("s")
          .create
          .map { histogram =>
            /** Times a Skunk query, recording a `db.client.operation.duration`
              * measurement tagged with `db.system`/`db.operation` (OTel
              * semantic-convention names), plus `error.type` if it fails - this
              * is this service's only Postgres consumer, so it's instrumented
              * directly here rather than via a new purerest combinator.
              */
            def timed[A](operation: String)(fa: F[A]): F[A] =
              for {
                start <- Async[F].monotonic
                result <- fa.attempt
                end <- Async[F].monotonic
                outcomeAttributes = result match {
                  case Right(_)    => Nil
                  case Left(error) =>
                    List(Attribute("error.type", error.getClass.getName))
                }
                _ <- histogram.record(
                  (end - start).toUnit(SECONDS),
                  List(
                    Attribute("db.system", "postgresql"),
                    Attribute("db.operation", operation)
                  ) ++ outcomeAttributes
                )
                a <- result.liftTo[F]
              } yield a

            new FulfilmentStore[F] {
              def create( /* codegen:fields:CREATE_PARAMS */ ): F[Fulfilment] =
                for {
                  id <- Sync[F].delay(UUID.randomUUID())
                  timestamps <- timed("insert") {
                    pool.use { session =>
                      session
                        .prepare(insertFulfilment)
                        .flatMap(
                          _.unique(
                            (id /* codegen:fields:SQL_INSERT_TUPLE_ARGS */ )
                          )
                        )
                    }
                  }
                } yield {
                  val (createdAt, updatedAt) = timestamps
                  Fulfilment(
                    id.toString,
                    /* codegen:fields:CONSTRUCT_ARGS */
                    createdAt.toInstant,
                    updatedAt.toInstant
                  )
                }

              def get(id: String): F[Option[Fulfilment]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    for {
                      row <- timed("select") {
                        pool.use { session =>
                          session
                            .prepare(selectFulfilment)
                            .flatMap(_.option(uuid))
                        }
                      }
                    } yield row.map {
                      case (
                            /* codegen:fields:SQL_SELECT_PATTERN_VARS */ createdAt,
                            updatedAt
                          ) =>
                        Fulfilment(
                          id,
                          /* codegen:fields:SQL_SELECT_CONSTRUCT_ARGS */
                          createdAt.toInstant,
                          updatedAt.toInstant
                        )
                    }
                }

              def update(
                  id: String
                  /* codegen:fields:UPDATE_PARAMS */
              ): F[Option[Fulfilment]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    for {
                      row <- timed("update") {
                        pool.use { session =>
                          session
                            .prepare(updateFulfilment)
                            .flatMap(
                              _.option(
                                ( /* codegen:fields:SQL_UPDATE_TUPLE_ARGS */ uuid)
                              )
                            )
                        }
                      }
                    } yield row.map {
                      case (
                            /* codegen:fields:SQL_SELECT_PATTERN_VARS */ createdAt,
                            updatedAt
                          ) =>
                        Fulfilment(
                          id,
                          /* codegen:fields:SQL_SELECT_CONSTRUCT_ARGS */
                          createdAt.toInstant,
                          updatedAt.toInstant
                        )
                    }
                }

              def delete(id: String): F[Boolean] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(false)
                  case Some(uuid) =>
                    timed("delete") {
                      pool.use { session =>
                        session
                          .prepare(deleteFulfilment)
                          .flatMap(_.option(uuid))
                          .map(_.isDefined)
                      }
                    }
                }

              def list(
                  limit: Int,
                  offset: Int
              ): F[(List[Fulfilment], Long)] =
                for {
                  rows <- timed("list") {
                    pool.use { session =>
                      session
                        .prepare(selectFulfilmentPage)
                        .flatMap(_.stream((limit, offset), 1024).compile.toList)
                    }
                  }
                  total <- rows.headOption match {
                    case Some(row) =>
                      // The row's last element is always count(*) OVER()'s
                      // total_count; using productElement keeps this working
                      // regardless of how many field-spec columns precede it.
                      Sync[F].pure(
                        row
                          .productElement(row.productArity - 1)
                          .asInstanceOf[Long]
                      )
                    case None =>
                      timed("count") { pool.use(_.unique(countFulfilment)) }
                  }
                } yield rows.map {
                  case (
                        id,
                        /* codegen:fields:SQL_SELECT_PATTERN_VARS */ createdAt,
                        updatedAt,
                        _
                      ) =>
                    Fulfilment(
                      id.toString,
                      /* codegen:fields:SQL_SELECT_CONSTRUCT_ARGS */
                      createdAt.toInstant,
                      updatedAt.toInstant
                    )
                } -> total

              def ping: F[Boolean] =
                timed("ping") {
                  pool.use(_.unique(pingQuery))
                }.attempt.map(_.isRight)
            }
          }
      }
  }
}
