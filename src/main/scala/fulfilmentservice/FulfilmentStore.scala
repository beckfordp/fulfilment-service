package fulfilmentservice

import cats.effect.{Async, Clock, Concurrent, Ref, Resource}
import cats.effect.std.UUIDGen
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import fulfilmentservice.domain.{
  FulfilmentCommand,
  FulfilmentFailure,
  FulfilmentState,
  OrderId
}
import io.circe.syntax._
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import purerest.metrics.LatencyBuckets
import skunk.Session
import skunk.circe.codec.all.jsonb
import skunk.codec.all._
import skunk.implicits._

import java.time.{OffsetDateTime, ZoneOffset}
import java.util.UUID
import scala.concurrent.duration.SECONDS

trait FulfilmentStore[F[_]] {
  def ping: F[Boolean]

  /** Idempotent per orderId: a redelivered PaymentAuthorized returns the
    * existing task unchanged.
    */
  def createTask(
      orderId: OrderId,
      items: List[FulfilmentItem],
      shippingAddress: Address
  ): F[FulfilmentTask]

  def get(orderId: OrderId): F[Option[FulfilmentTask]]

  def list(state: Option[FulfilmentState]): F[List[FulfilmentTask]]

  /** Applies `command` via the pure lifecycle (`FulfilmentTask.applyCommand`)
    * atomically per task; `Dispatch`/`Fail` also write their outbox event in
    * the same step.
    */
  def transition(
      orderId: OrderId,
      command: FulfilmentCommand
  ): F[Either[FulfilmentFailure, FulfilmentTask]]
}

object FulfilmentStore {

  /** Test/dev interpreter: `Ref`-backed, timestamps from `Clock[F]`. The outbox
    * append follows the task update (not atomic with it) - fine for tests.
    */
  def inMemory[F[_]: Concurrent: Clock: UUIDGen]: F[FulfilmentStore[F]] =
    inMemoryWithOutbox[F].map(_._1)

  def inMemoryWithOutbox[F[_]: Concurrent: Clock: UUIDGen]
      : F[(FulfilmentStore[F], Ref[F, List[(String, EventEnvelope)]])] =
    for {
      tasks <- Ref.of[F, Map[OrderId, FulfilmentTask]](Map.empty)
      outbox <- Ref.of[F, List[(String, EventEnvelope)]](Nil)
    } yield {
      val store = new FulfilmentStore[F] {
        def ping: F[Boolean] = Concurrent[F].pure(true)

        def createTask(
            orderId: OrderId,
            items: List[FulfilmentItem],
            shippingAddress: Address
        ): F[FulfilmentTask] =
          Clock[F].realTimeInstant.flatMap { now =>
            tasks.modify { current =>
              current.get(orderId) match {
                case Some(existing) => (current, existing)
                case None           =>
                  val task = FulfilmentTask(
                    orderId,
                    FulfilmentState.Pending,
                    items,
                    shippingAddress,
                    None,
                    None,
                    None,
                    now,
                    now
                  )
                  (current + (orderId -> task), task)
              }
            }
          }

        def get(orderId: OrderId): F[Option[FulfilmentTask]] =
          tasks.get.map(_.get(orderId))

        def list(state: Option[FulfilmentState]): F[List[FulfilmentTask]] =
          tasks.get.map(
            _.values.toList
              .filter(task => state.forall(_ == task.state))
              .sortBy(_.createdAt)
          )

        def transition(
            orderId: OrderId,
            command: FulfilmentCommand
        ): F[Either[FulfilmentFailure, FulfilmentTask]] =
          Clock[F].realTimeInstant.flatMap { now =>
            tasks
              .modify { current =>
                current.get(orderId) match {
                  case None => (current, Left(FulfilmentFailure.NotFound))
                  case Some(existing) =>
                    FulfilmentTask.applyCommand(existing, command, now) match {
                      case Left(error)    => (current, Left(error))
                      case Right(updated) =>
                        (current + (orderId -> updated), Right(updated))
                    }
                }
              }
              .flatMap {
                case Right(updated) =>
                  FulfilmentEvents
                    .forCommand[F](orderId, updated, command)
                    .flatMap {
                      case Some(envelope) =>
                        outbox
                          .update(_ :+ (OrderId.render(orderId) -> envelope))
                          .as(Right(updated))
                      case None => Concurrent[F].pure(Right(updated))
                    }
                case Left(error) => Concurrent[F].pure(Left(error))
              }
          }
      }
      (store, outbox)
    }

  private type TaskRow = (
      UUID,
      FulfilmentState,
      List[FulfilmentItem],
      Address,
      Option[String],
      Option[String],
      Option[String],
      OffsetDateTime,
      OffsetDateTime
  )

  private val stateCodec: skunk.Codec[FulfilmentState] =
    text.eimap(value =>
      FulfilmentState
        .fromString(value)
        .toRight(s"Unknown fulfilment state: $value")
    )(_.wire)

  private val selectTask: skunk.Query[UUID, TaskRow] =
    sql"""
      SELECT order_id, state, items, shipping_address, tracking_no, carrier, failure_reason, created_at, updated_at FROM "fulfilment_task" WHERE order_id = $uuid
    """.query(
      uuid *: stateCodec *: jsonb[List[FulfilmentItem]] *:
        jsonb[Address] *: text.opt *: text.opt *: text.opt *:
        timestamptz *: timestamptz
    )

  private val selectTaskForUpdate: skunk.Query[UUID, TaskRow] =
    sql"""
      SELECT order_id, state, items, shipping_address, tracking_no, carrier, failure_reason, created_at, updated_at FROM "fulfilment_task" WHERE order_id = $uuid FOR UPDATE
    """.query(
      uuid *: stateCodec *: jsonb[List[FulfilmentItem]] *:
        jsonb[Address] *: text.opt *: text.opt *: text.opt *:
        timestamptz *: timestamptz
    )

  private val selectAllTasks: skunk.Query[skunk.Void, TaskRow] =
    sql"""
      SELECT order_id, state, items, shipping_address, tracking_no, carrier, failure_reason, created_at, updated_at FROM "fulfilment_task" ORDER BY created_at
    """.query(
      uuid *: stateCodec *: jsonb[List[FulfilmentItem]] *:
        jsonb[Address] *: text.opt *: text.opt *: text.opt *:
        timestamptz *: timestamptz
    )

  private val selectTasksByState: skunk.Query[FulfilmentState, TaskRow] =
    sql"""
      SELECT order_id, state, items, shipping_address, tracking_no, carrier, failure_reason, created_at, updated_at FROM "fulfilment_task" WHERE state = $stateCodec ORDER BY created_at
    """.query(
      uuid *: stateCodec *: jsonb[List[FulfilmentItem]] *:
        jsonb[Address] *: text.opt *: text.opt *: text.opt *:
        timestamptz *: timestamptz
    )

  private val insertTask
      : skunk.Query[(UUID, List[FulfilmentItem], Address), TaskRow] =
    sql"""
      INSERT INTO "fulfilment_task" (order_id, state, items, shipping_address)
      VALUES ($uuid, 'PENDING', ${jsonb[List[FulfilmentItem]]}, ${jsonb[
        Address
      ]})
      ON CONFLICT (order_id) DO NOTHING
      RETURNING order_id, state, items, shipping_address, tracking_no, carrier, failure_reason, created_at, updated_at
    """.query(
      uuid *: stateCodec *: jsonb[List[FulfilmentItem]] *:
        jsonb[Address] *: text.opt *: text.opt *: text.opt *:
        timestamptz *: timestamptz
    )

  private val updateTaskState: skunk.Query[
    (FulfilmentState, Option[String], Option[String], Option[String], UUID),
    TaskRow
  ] =
    sql"""
      UPDATE "fulfilment_task"
      SET state = $stateCodec, tracking_no = ${text.opt}, carrier = ${text.opt},
          failure_reason = ${text.opt}, updated_at = now()
      WHERE order_id = $uuid
      RETURNING order_id, state, items, shipping_address, tracking_no, carrier, failure_reason, created_at, updated_at
    """.query(
      uuid *: stateCodec *: jsonb[List[FulfilmentItem]] *:
        jsonb[Address] *: text.opt *: text.opt *: text.opt *:
        timestamptz *: timestamptz
    )

  private val insertOutboxEvent
      : skunk.Command[(UUID, String, EventEnvelope, String)] =
    sql"""
      INSERT INTO outbox_events (id, aggregate_id, payload, correlation_id)
      VALUES ($uuid, $text, ${jsonb[EventEnvelope]}, $text)
    """.command

  private val pingQuery: skunk.Query[skunk.Void, Int] =
    sql"SELECT 1".query(int4)

  private def toTask(row: TaskRow): FulfilmentTask = {
    val (
      orderId,
      state,
      items,
      address,
      trackingNo,
      carrier,
      failureReason,
      createdAt,
      updatedAt
    ) = row
    FulfilmentTask(
      OrderId.fromUuid(orderId),
      state,
      items,
      address,
      trackingNo,
      carrier,
      failureReason,
      createdAt.toInstant,
      updatedAt.toInstant
    )
  }

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
          .withExplicitBucketBoundaries(LatencyBuckets.seconds)
          .create
          .map { histogram =>
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
              def ping: F[Boolean] =
                timed("ping")(pool.use(_.unique(pingQuery))).attempt
                  .map(_.isRight)

              def createTask(
                  orderId: OrderId,
                  items: List[FulfilmentItem],
                  shippingAddress: Address
              ): F[FulfilmentTask] =
                timed("insert") {
                  pool.use { session =>
                    for {
                      id = OrderId.uuid(orderId)
                      inserted <- session
                        .prepare(insertTask)
                        .flatMap(_.option((id, items, shippingAddress)))
                      task <- inserted match {
                        case Some(row) => Async[F].pure(toTask(row))
                        case None      =>
                          session
                            .prepare(selectTask)
                            .flatMap(_.unique(id))
                            .map(toTask)
                      }
                    } yield task
                  }
                }

              def get(orderId: OrderId): F[Option[FulfilmentTask]] =
                timed("select") {
                  pool.use { session =>
                    session
                      .prepare(selectTask)
                      .flatMap(_.option(OrderId.uuid(orderId)))
                  }
                }.map(_.map(toTask))

              def list(
                  state: Option[FulfilmentState]
              ): F[List[FulfilmentTask]] =
                timed("select") {
                  pool.use { session =>
                    state match {
                      case None =>
                        session
                          .prepare(selectAllTasks)
                          .flatMap(_.stream(skunk.Void, 64).compile.toList)
                      case Some(filter) =>
                        session
                          .prepare(selectTasksByState)
                          .flatMap(_.stream(filter, 64).compile.toList)
                    }
                  }
                }.map(_.map(toTask))

              def transition(
                  orderId: OrderId,
                  command: FulfilmentCommand
              ): F[Either[FulfilmentFailure, FulfilmentTask]] = {
                val id = OrderId.uuid(orderId)
                timed("update") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      (
                        session
                          .prepare(selectTaskForUpdate)
                          .flatMap(_.option(id)),
                        Async[F].realTimeInstant
                      ).tupled
                        .flatMap {
                          case (None, _) =>
                            Async[F].pure(Left(FulfilmentFailure.NotFound))
                          case (Some(row), now) =>
                            FulfilmentTask.applyCommand(
                              toTask(row),
                              command,
                              now
                            ) match {
                              case Left(error) =>
                                Async[F].pure(Left(error))
                              case Right(next) =>
                                for {
                                  updated <- session
                                    .prepare(updateTaskState)
                                    .flatMap(
                                      _.unique(
                                        (
                                          next.state,
                                          next.trackingNo,
                                          next.carrier,
                                          next.failureReason,
                                          id
                                        )
                                      )
                                    )
                                    .map(toTask)
                                  event <- FulfilmentEvents
                                    .forCommand[F](orderId, updated, command)
                                  _ <- event.traverse_ { envelope =>
                                    UUIDGen[F].randomUUID
                                      .flatMap { outboxId =>
                                        session
                                          .prepare(insertOutboxEvent)
                                          .flatMap(
                                            _.execute(
                                              (
                                                outboxId,
                                                OrderId.render(orderId),
                                                envelope,
                                                envelope.correlationId
                                              )
                                            ).void
                                          )
                                      }
                                  }
                                } yield Right(updated)
                            }
                        }
                    }
                  }
                }
              }
            }
          }
      }
  }
}
