package fulfilmentservice

import cats.{FlatMap, Functor}
import cats.effect.{Async, Clock, Concurrent, Ref, Resource}
import cats.effect.std.UUIDGen
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import fulfilmentservice.domain.{
  FulfilmentCommand,
  FulfilmentFailure,
  FulfilmentState,
  OrderId,
  Reconciliation,
  ReconciliationStatus,
  ResolveReconciliationOutcome
}
import io.circe.syntax._
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import purekafka.tracing.KafkaTracing
import purerest.metrics.{LatencyBuckets, PoolMetrics}
import skunk.Session
import skunk.circe.codec.all.jsonb
import skunk.codec.all._
import skunk.implicits._

import java.time.{Instant, OffsetDateTime, ZoneOffset}
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
    * the same step. Returns the task as read under the lock and as updated, so
    * telemetry sees the true `from` state (callers use `transition`).
    */
  def transitionFrom(
      orderId: OrderId,
      command: FulfilmentCommand
  ): F[Either[FulfilmentFailure, (FulfilmentTask, FulfilmentTask)]]

  /** Records a manual-reconciliation flag (US-21.4), first-wins. False for an
    * unknown task.
    */
  def flagReconciliation(orderId: OrderId, reason: String): F[Boolean]

  def reconciliationReason(orderId: OrderId): F[Option[String]]

  /** The open/resolved reconciliation queue (ADM-4); newest first. */
  def listReconciliations(
      status: Option[ReconciliationStatus],
      orderId: Option[OrderId]
  ): F[List[Reconciliation]]

  /** Resolves an open flag; the first resolver wins and a repeat returns the
    * existing row untouched.
    */
  def resolveReconciliation(
      orderId: OrderId,
      operatorId: String,
      note: Option[String],
      resolvedAt: Instant
  ): F[ResolveReconciliationOutcome]
}

object FulfilmentStore {

  extension [F[_]: Functor](store: FulfilmentStore[F]) {

    /** `transitionFrom`, keeping only the updated task. */
    def transition(
        orderId: OrderId,
        command: FulfilmentCommand
    ): F[Either[FulfilmentFailure, FulfilmentTask]] =
      store.transitionFrom(orderId, command).map(_.map(_._2))
  }

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
      reconciliation <- Ref.of[F, Map[OrderId, Reconciliation]](Map.empty)
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

        def transitionFrom(
            orderId: OrderId,
            command: FulfilmentCommand
        ): F[Either[FulfilmentFailure, (FulfilmentTask, FulfilmentTask)]] =
          Clock[F].realTimeInstant.flatMap { now =>
            tasks
              .modify { current =>
                current.get(orderId) match {
                  case None => (current, Left(FulfilmentFailure.NotFound))
                  case Some(existing) =>
                    FulfilmentTask.applyCommand(existing, command, now) match {
                      case Left(error)    => (current, Left(error))
                      case Right(updated) =>
                        (
                          current + (orderId -> updated),
                          Right((existing, updated))
                        )
                    }
                }
              }
              .flatMap {
                case Right((existing, updated)) =>
                  FulfilmentEvents
                    .forCommand[F](orderId, updated, command)
                    .flatMap {
                      case Some(envelope) =>
                        outbox
                          .update(_ :+ (OrderId.render(orderId) -> envelope))
                          .as(Right((existing, updated)))
                      case None =>
                        Concurrent[F].pure(Right((existing, updated)))
                    }
                case Left(error) => Concurrent[F].pure(Left(error))
              }
          }

        def flagReconciliation(
            orderId: OrderId,
            reason: String
        ): F[Boolean] =
          tasks.get.map(_.contains(orderId)).flatMap {
            case false => Concurrent[F].pure(false)
            case true  =>
              Clock[F].realTimeInstant.flatMap { now =>
                reconciliation.modify { rows =>
                  if (rows.contains(orderId)) (rows, false)
                  else
                    (
                      rows + (orderId -> Reconciliation(
                        orderId,
                        reason,
                        ReconciliationStatus.Open,
                        now
                      )),
                      true
                    )
                }
              }
          }

        def reconciliationReason(orderId: OrderId): F[Option[String]] =
          reconciliation.get.map(_.get(orderId).map(_.reason))

        def listReconciliations(
            status: Option[ReconciliationStatus],
            orderId: Option[OrderId]
        ): F[List[Reconciliation]] =
          reconciliation.get.map(
            _.values
              .filter(row => status.forall(_ == row.status))
              .filter(row => orderId.forall(_ == row.orderId))
              .toList
              .sortBy(row => (row.createdAt, OrderId.render(row.orderId)))(using
                Ordering[(Instant, String)].reverse
              )
          )

        def resolveReconciliation(
            orderId: OrderId,
            operatorId: String,
            note: Option[String],
            resolvedAt: Instant
        ): F[ResolveReconciliationOutcome] =
          reconciliation.modify { rows =>
            rows.get(orderId) match {
              case None      => (rows, ResolveReconciliationOutcome.NotFound)
              case Some(row) =>
                row.resolve(operatorId, note, resolvedAt) match {
                  case Some(resolved) =>
                    (
                      rows.updated(orderId, resolved),
                      ResolveReconciliationOutcome.Resolved(resolved)
                    )
                  case None =>
                    (
                      rows,
                      ResolveReconciliationOutcome.AlreadyResolved(row)
                    )
                }
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

  // `trace_parent` is the active span's W3C traceparent (PERF-5.4); Debezium
  // lifts it to a `traceparent` header. NULL when written outside a span.
  private val insertOutboxEvent
      : skunk.Command[(UUID, String, EventEnvelope, String, Option[String])] =
    sql"""
      INSERT INTO outbox_events (id, aggregate_id, payload, correlation_id, trace_parent)
      VALUES ($uuid, $text, ${jsonb[EventEnvelope]}, $text, ${text.opt})
    """.command

  /** Writes one outbox row in `session`'s transaction inside an `outbox.write`
    * span (PERF-5.9), stamped with that span's trace context
    * (`KafkaTracing.traceParent`) so consumers become its children.
    */
  private def insertOutbox[F[_]: FlatMap](
      tracer: Tracer[F],
      session: Session[F],
      row: (UUID, String, EventEnvelope, String)
  ): F[Unit] =
    tracer
      .span(
        "outbox.write",
        Attribute("outbox.event_type", row._3.eventType),
        Attribute("outbox.aggregate_id", row._2)
      )
      .surround(
        KafkaTracing
          .traceParent(tracer)
          .flatMap(tp =>
            session
              .prepare(insertOutboxEvent)
              .flatMap(_.execute(row :* tp))
              .void
          )
      )

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

  private val insertReconciliationIfTaskExists
      : skunk.Query[(UUID, String, UUID), UUID] =
    sql"""
      INSERT INTO fulfilment_reconciliation (order_id, reason)
      SELECT $uuid, $text
      WHERE EXISTS (SELECT 1 FROM "fulfilment_task" WHERE order_id = $uuid)
      ON CONFLICT (order_id) DO NOTHING
      RETURNING order_id
    """.query(uuid)

  private val selectReconciliationReason: skunk.Query[UUID, String] =
    sql"""
      SELECT reason FROM fulfilment_reconciliation WHERE order_id = $uuid
    """.query(text)

  private val optionalText: skunk.Codec[Option[String]] = text.opt

  private val reconciliationStatusCodec: skunk.Codec[ReconciliationStatus] =
    text.eimap(ReconciliationStatus.fromString)(_.asString)

  private val reconciliationRow: skunk.Codec[
    (
        UUID,
        String,
        ReconciliationStatus,
        OffsetDateTime,
        Option[String],
        Option[String],
        Option[OffsetDateTime]
    )
  ] =
    uuid *: text *: reconciliationStatusCodec *: timestamptz *: text.opt *:
      text.opt *: timestamptz.opt

  private def toReconciliation(
      row: (
          UUID,
          String,
          ReconciliationStatus,
          OffsetDateTime,
          Option[String],
          Option[String],
          Option[OffsetDateTime]
      )
  ): Reconciliation = {
    val (orderId, reason, status, createdAt, operatorId, note, resolvedAt) =
      row
    Reconciliation(
      OrderId.fromUuid(orderId),
      reason,
      status,
      createdAt.toInstant,
      operatorId,
      note,
      resolvedAt.map(_.toInstant)
    )
  }

  private val selectReconciliations: skunk.Query[
    (Option[String], Option[String], Option[String], Option[String]),
    (
        UUID,
        String,
        ReconciliationStatus,
        OffsetDateTime,
        Option[String],
        Option[String],
        Option[OffsetDateTime]
    )
  ] =
    sql"""
      SELECT order_id, reason, status, created_at, operator_id, note, resolved_at
      FROM fulfilment_reconciliation
      WHERE ($optionalText IS NULL OR status = $optionalText)
        AND ($optionalText IS NULL OR order_id = $optionalText::uuid)
      ORDER BY created_at DESC, order_id
    """.query(reconciliationRow)

  private val selectReconciliationByOrderId: skunk.Query[
    UUID,
    (
        UUID,
        String,
        ReconciliationStatus,
        OffsetDateTime,
        Option[String],
        Option[String],
        Option[OffsetDateTime]
    )
  ] =
    sql"""
      SELECT order_id, reason, status, created_at, operator_id, note, resolved_at
      FROM fulfilment_reconciliation
      WHERE order_id = $uuid
    """.query(reconciliationRow)

  private val resolveReconciliationQuery: skunk.Query[
    (String, Option[String], OffsetDateTime, UUID),
    (
        UUID,
        String,
        ReconciliationStatus,
        OffsetDateTime,
        Option[String],
        Option[String],
        Option[OffsetDateTime]
    )
  ] =
    sql"""
      UPDATE fulfilment_reconciliation
      SET status = 'resolved',
          operator_id = $text,
          note = $optionalText,
          resolved_at = $timestamptz
      WHERE order_id = $uuid
        AND status = 'open'
      RETURNING order_id, reason, status, created_at, operator_id, note,
                resolved_at
    """.query(reconciliationRow)

  def postgres[F[_]: Async: Console: Network](
      config: PostgresConfig,
      meter: Meter[F],
      tracer: Tracer[F]
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
      .evalMap(PoolMetrics.instrument(meter, "fulfilment")(_))
      .evalMap { pool =>
        meter
          .histogram[Double]("db.client.operation.duration")
          .withUnit("s")
          .withExplicitBucketBoundaries(LatencyBuckets.seconds)
          .create
          .flatMap { histogram =>
            meter
              .counter[Long]("reconciliation.flags.recorded")
              .withDescription(
                "Manual-reconciliation flags actually recorded (first-wins)."
              )
              .create
              .map { flagsCounter =>
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

                  def flagReconciliation(
                      orderId: OrderId,
                      reason: String
                  ): F[Boolean] = {
                    val id = OrderId.uuid(orderId)
                    timed("flag_reconciliation") {
                      pool.use { session =>
                        session
                          .prepare(insertReconciliationIfTaskExists)
                          .flatMap(_.option((id, reason, id)))
                          .map(_.isDefined)
                      }
                    }.flatMap {
                      case true =>
                        flagsCounter
                          .add(1L, List(Attribute("reason", reason)))
                          .attempt
                          .void
                          .as(true)
                      case false => Async[F].pure(false)
                    }
                  }

                  def listReconciliations(
                      status: Option[ReconciliationStatus],
                      orderIdFilter: Option[OrderId]
                  ): F[List[Reconciliation]] =
                    timed("select_reconciliations") {
                      pool.use(
                        _.execute(selectReconciliations)(
                          (
                            status.map(_.asString),
                            status.map(_.asString),
                            orderIdFilter.map(OrderId.render),
                            orderIdFilter.map(OrderId.render)
                          )
                        )
                      )
                    }.map(_.map(toReconciliation))

                  def resolveReconciliation(
                      orderId: OrderId,
                      operatorId: String,
                      note: Option[String],
                      resolvedAt: Instant
                  ): F[ResolveReconciliationOutcome] =
                    timed("resolve_reconciliation") {
                      pool.use { session =>
                        session.transaction.use { _ =>
                          session
                            .prepare(resolveReconciliationQuery)
                            .flatMap(
                              _.option(
                                (
                                  operatorId,
                                  note,
                                  OffsetDateTime.ofInstant(
                                    resolvedAt,
                                    ZoneOffset.UTC
                                  ),
                                  OrderId.uuid(orderId)
                                )
                              )
                            )
                            .flatMap {
                              case Some(row) =>
                                Async[F].pure(
                                  ResolveReconciliationOutcome.Resolved(
                                    toReconciliation(row)
                                  )
                                )
                              case None =>
                                session
                                  .prepare(selectReconciliationByOrderId)
                                  .flatMap(_.option(OrderId.uuid(orderId)))
                                  .map {
                                    case Some(row) =>
                                      ResolveReconciliationOutcome
                                        .AlreadyResolved(
                                          toReconciliation(row)
                                        )
                                    case None =>
                                      ResolveReconciliationOutcome.NotFound
                                  }
                            }
                        }
                      }
                    }

                  def reconciliationReason(
                      orderId: OrderId
                  ): F[Option[String]] =
                    timed("select_reconciliation_reason") {
                      pool.use { session =>
                        session
                          .prepare(selectReconciliationReason)
                          .flatMap(_.option(OrderId.uuid(orderId)))
                      }
                    }

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

                  def transitionFrom(
                      orderId: OrderId,
                      command: FulfilmentCommand
                  ): F[
                    Either[FulfilmentFailure, (FulfilmentTask, FulfilmentTask)]
                  ] = {
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
                                val before = toTask(row)
                                FulfilmentTask.applyCommand(
                                  before,
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
                                        .forCommand[F](
                                          orderId,
                                          updated,
                                          command
                                        )
                                      _ <- event.traverse_ { envelope =>
                                        UUIDGen[F].randomUUID
                                          .flatMap { outboxId =>
                                            insertOutbox(
                                              tracer,
                                              session,
                                              (
                                                outboxId,
                                                OrderId.render(orderId),
                                                envelope,
                                                envelope.correlationId
                                              )
                                            )
                                          }
                                      }
                                    } yield Right((before, updated))
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
}
