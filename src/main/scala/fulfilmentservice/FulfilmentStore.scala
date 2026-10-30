package fulfilmentservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import fulfilmentservice.domain.OrderId
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

  def startPicking(
      orderId: OrderId
  ): F[Either[FulfilmentFailure, FulfilmentTask]]

  def pack(orderId: OrderId): F[Either[FulfilmentFailure, FulfilmentTask]]

  def dispatch(
      orderId: OrderId,
      trackingNo: String,
      carrier: String
  ): F[Either[FulfilmentFailure, FulfilmentTask]]

  def fail(
      orderId: OrderId,
      reason: String
  ): F[Either[FulfilmentFailure, FulfilmentTask]]
}

object FulfilmentStore {

  private def invalidTransition(order: FulfilmentTask): FulfilmentFailure =
    FulfilmentFailure.InvalidTransition(
      s"Cannot transition order ${order.orderId} from ${order.state.wire}"
    )

  private def nonTerminalAllowed(
      order: FulfilmentTask
  ): Either[FulfilmentFailure, Unit] =
    if (FulfilmentState.isTerminal(order.state))
      Left(invalidTransition(order))
    else Right(())

  def inMemory[F[_]: Sync]: F[FulfilmentStore[F]] =
    inMemoryWithOutbox[F].map(_._1)

  def inMemoryWithOutbox[F[_]: Sync]
      : F[(FulfilmentStore[F], Ref[F, List[(String, EventEnvelope)]])] =
    for {
      tasks <- Ref.of[F, Map[OrderId, FulfilmentTask]](Map.empty)
      outbox <- Ref.of[F, List[(String, EventEnvelope)]](Nil)
    } yield {
      def transition(
          orderId: OrderId,
          update: FulfilmentTask => Either[FulfilmentFailure, FulfilmentTask],
          event: FulfilmentTask => F[Option[EventEnvelope]]
      ): F[Either[FulfilmentFailure, FulfilmentTask]] =
        tasks
          .modify { current =>
            current.get(orderId) match {
              case None           => (current, Left(FulfilmentFailure.NotFound))
              case Some(existing) =>
                update(existing) match {
                  case Left(error)    => (current, Left(error))
                  case Right(updated) =>
                    (current + (orderId -> updated), Right(updated))
                }
            }
          }
          .flatMap {
            case Right(updated) =>
              event(updated).flatMap {
                case Some(envelope) =>
                  outbox
                    .update(_ :+ (OrderId.render(orderId) -> envelope))
                    .as(Right(updated))
                case None => Sync[F].pure(Right(updated))
              }
            case Left(error) => Sync[F].pure(Left(error))
          }

      val store = new FulfilmentStore[F] {
        def ping: F[Boolean] = Sync[F].pure(true)

        def createTask(
            orderId: OrderId,
            items: List[FulfilmentItem],
            shippingAddress: Address
        ): F[FulfilmentTask] =
          tasks.modify { current =>
            current.get(orderId) match {
              case Some(existing) => (current, existing)
              case None           =>
                val now = java.time.Instant.now()
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

        def get(orderId: OrderId): F[Option[FulfilmentTask]] =
          tasks.get.map(_.get(orderId))

        def list(state: Option[FulfilmentState]): F[List[FulfilmentTask]] =
          tasks.get.map(
            _.values.toList
              .filter(task => state.forall(_ == task.state))
              .sortBy(_.createdAt)
          )

        def startPicking(
            orderId: OrderId
        ): F[Either[FulfilmentFailure, FulfilmentTask]] =
          transition(
            orderId,
            task =>
              if (task.state == FulfilmentState.Pending)
                Right(
                  task.copy(
                    state = FulfilmentState.Picking,
                    updatedAt = java.time.Instant.now()
                  )
                )
              else Left(invalidTransition(task)),
            _ => Sync[F].pure(None)
          )

        def pack(
            orderId: OrderId
        ): F[Either[FulfilmentFailure, FulfilmentTask]] =
          transition(
            orderId,
            task =>
              if (task.state == FulfilmentState.Picking)
                Right(
                  task.copy(
                    state = FulfilmentState.Packed,
                    updatedAt = java.time.Instant.now()
                  )
                )
              else Left(invalidTransition(task)),
            _ => Sync[F].pure(None)
          )

        def dispatch(
            orderId: OrderId,
            trackingNo: String,
            carrier: String
        ): F[Either[FulfilmentFailure, FulfilmentTask]] = {
          val tracking = trackingNo.trim
          val carrierName = carrier.trim
          if (tracking.isEmpty || carrierName.isEmpty)
            Sync[F].pure(
              Left(
                FulfilmentFailure.InvalidRequest(
                  "trackingNo and carrier are required"
                )
              )
            )
          else
            transition(
              orderId,
              task =>
                if (task.state == FulfilmentState.Packed)
                  Right(
                    task.copy(
                      state = FulfilmentState.Dispatched,
                      trackingNo = Some(tracking),
                      carrier = Some(carrierName),
                      updatedAt = java.time.Instant.now()
                    )
                  )
                else Left(invalidTransition(task)),
              task =>
                FulfilmentEvents
                  .dispatchConfirmed[F](orderId, tracking, task.items)
                  .map(Some(_))
            )
        }

        def fail(
            orderId: OrderId,
            reason: String
        ): F[Either[FulfilmentFailure, FulfilmentTask]] = {
          val failureReason = reason.trim
          if (failureReason.isEmpty)
            Sync[F].pure(
              Left(FulfilmentFailure.InvalidRequest("reason is required"))
            )
          else
            transition(
              orderId,
              task =>
                nonTerminalAllowed(task).map(_ =>
                  task.copy(
                    state = FulfilmentState.DispatchFailed,
                    failureReason = Some(failureReason),
                    updatedAt = java.time.Instant.now()
                  )
                ),
              task =>
                FulfilmentEvents
                  .dispatchFailed[F](orderId, failureReason)
                  .map(Some(_))
            )
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
                        case Some(row) => Sync[F].pure(toTask(row))
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
                    session
                      .prepare(selectAllTasks)
                      .flatMap(_.stream(skunk.Void, 64).compile.toList)
                  }
                }.map(
                  _.map(toTask).filter(task => state.forall(_ == task.state))
                )

              private def transition(
                  orderId: OrderId,
                  update: FulfilmentTask => Either[
                    FulfilmentFailure,
                    FulfilmentTask
                  ],
                  event: FulfilmentTask => F[Option[EventEnvelope]]
              ): F[Either[FulfilmentFailure, FulfilmentTask]] = {
                val id = OrderId.uuid(orderId)
                timed("update") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      session
                        .prepare(selectTaskForUpdate)
                        .flatMap(_.option(id))
                        .flatMap {
                          case None =>
                            Sync[F].pure(
                              Left(FulfilmentFailure.NotFound)
                            )
                          case Some(row) =>
                            val current = toTask(row)
                            update(current) match {
                              case Left(error) =>
                                Sync[F].pure(Left(error))
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
                                  _ <- event(updated).flatMap {
                                    case None           => Sync[F].unit
                                    case Some(envelope) =>
                                      Sync[F]
                                        .delay(UUID.randomUUID())
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

              def startPicking(
                  orderId: OrderId
              ): F[Either[FulfilmentFailure, FulfilmentTask]] =
                transition(
                  orderId,
                  task =>
                    if (task.state == FulfilmentState.Pending)
                      Right(task.copy(state = FulfilmentState.Picking))
                    else Left(invalidTransition(task)),
                  _ => Sync[F].pure(None)
                )

              def pack(
                  orderId: OrderId
              ): F[Either[FulfilmentFailure, FulfilmentTask]] =
                transition(
                  orderId,
                  task =>
                    if (task.state == FulfilmentState.Picking)
                      Right(task.copy(state = FulfilmentState.Packed))
                    else Left(invalidTransition(task)),
                  _ => Sync[F].pure(None)
                )

              def dispatch(
                  orderId: OrderId,
                  trackingNo: String,
                  carrier: String
              ): F[Either[FulfilmentFailure, FulfilmentTask]] = {
                val tracking = trackingNo.trim
                val carrierName = carrier.trim
                if (tracking.isEmpty || carrierName.isEmpty)
                  Sync[F].pure(
                    Left(
                      FulfilmentFailure.InvalidRequest(
                        "trackingNo and carrier are required"
                      )
                    )
                  )
                else
                  transition(
                    orderId,
                    task =>
                      if (task.state == FulfilmentState.Packed)
                        Right(
                          task.copy(
                            state = FulfilmentState.Dispatched,
                            trackingNo = Some(tracking),
                            carrier = Some(carrierName)
                          )
                        )
                      else Left(invalidTransition(task)),
                    task =>
                      FulfilmentEvents
                        .dispatchConfirmed[F](orderId, tracking, task.items)
                        .map(Some(_))
                  )
              }

              def fail(
                  orderId: OrderId,
                  reason: String
              ): F[Either[FulfilmentFailure, FulfilmentTask]] = {
                val failureReason = reason.trim
                if (failureReason.isEmpty)
                  Sync[F].pure(
                    Left(FulfilmentFailure.InvalidRequest("reason is required"))
                  )
                else
                  transition(
                    orderId,
                    task =>
                      nonTerminalAllowed(task).map(_ =>
                        task.copy(
                          state = FulfilmentState.DispatchFailed,
                          failureReason = Some(failureReason)
                        )
                      ),
                    task =>
                      FulfilmentEvents
                        .dispatchFailed[F](orderId, failureReason)
                        .map(Some(_))
                  )
              }
            }
          }
      }
  }
}
