package fulfilmentservice

import cats.Monad
import cats.effect.{Async, Clock, Resource}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import fulfilmentservice.domain.{
  FulfilmentCommand,
  FulfilmentFailure,
  FulfilmentState,
  OrderId
}
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{BucketBoundaries, Meter}
import purerest.logging.Milestones
import skunk.Session
import skunk.codec.all._
import skunk.implicits._

import java.time.Instant

/** Dispatch-team telemetry (gluon PERF-4.7, 4.11), as a `FulfilmentStore`
  * decorator — tasks are born in `createTask` and moved only by `transition`:
  *
  *   - `fulfilment.tasks.created`: new tasks (a redelivered PaymentAuthorized
  *     returns the existing task and isn't counted)
  *   - `fulfilment.transitions{from,to}`: dispatch rate is the rate of
  *     `to=DISPATCHED`
  *   - `fulfilment.step.duration{from,to}` (s): time the task spent in `from`
  *
  * Plus a `Milestones` line per step (`order_dispatched` on dispatch). This is
  * the *people* backlog, deliberately separate from consumer lag (Q14).
  */
object FulfilmentJourneyMetrics {

  /** 1s–1d: a demo dispatch bot moves in seconds, a backlog in hours. */
  val stepBuckets: BucketBoundaries = BucketBoundaries(
    1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
    21600.0, 43200.0, 86400.0
  )

  def instrument[F[_]: Monad](
      store: FulfilmentStore[F],
      meter: Meter[F],
      logger: StructuredLogger[F]
  ): F[FulfilmentStore[F]] =
    (
      meter.counter[Long]("fulfilment.tasks.created").create,
      meter.counter[Long]("fulfilment.transitions").create,
      meter
        .histogram[Double]("fulfilment.step.duration")
        .withUnit("s")
        .withExplicitBucketBoundaries(stepBuckets)
        .create
    ).mapN { (created, transitions, stepDuration) =>
      new FulfilmentStore[F] {
        export store.{createTask => _, transitionFrom => _, *}

        def createTask(
            orderId: OrderId,
            items: List[FulfilmentItem],
            shippingAddress: Address
        ): F[FulfilmentTask] =
          store.get(orderId).flatMap { before =>
            store.createTask(orderId, items, shippingAddress).flatTap { _ =>
              (created.inc() *> Milestones.log(logger)(
                "fulfilment_task_created",
                OrderId.render(orderId),
                None
              )).whenA(before.isEmpty)
            }
          }

        // `prev` is the row the store read under its lock, so a racing
        // transition can't mislabel `from` or the time in state.
        def transitionFrom(
            orderId: OrderId,
            command: FulfilmentCommand
        ): F[Either[FulfilmentFailure, (FulfilmentTask, FulfilmentTask)]] =
          store.transitionFrom(orderId, command).flatTap {
            case Right((prev, after)) =>
              val attrs = List(
                Attribute("from", prev.state.wire),
                Attribute("to", after.state.wire)
              )
              val name =
                if (after.state == FulfilmentState.Dispatched)
                  "order_dispatched"
                else s"fulfilment_${after.state.wire.toLowerCase}"
              transitions.inc(attrs) *>
                stepDuration.record(
                  math.max(
                    0L,
                    after.updatedAt.toEpochMilli - prev.updatedAt.toEpochMilli
                  ) / 1000.0,
                  attrs
                ) *>
                Milestones.log(logger)(
                  name,
                  OrderId.render(orderId),
                  None,
                  Map("from" -> prev.state.wire)
                )
            case Left(_) => Monad[F].unit
          }
      }
    }
}

/** Open tasks in one state: how many, and when the oldest entered it. */
final case class BacklogState(
    state: FulfilmentState,
    size: Long,
    oldestEnteredAt: Option[Instant]
)

/** Dispatch backlog gauges (gluon PERF-4.7, Q13/Q14):
  * `fulfilment.backlog.size{state}` and `fulfilment.backlog.oldest_age{state}`
  * (s) for the open states PENDING, PICKING and PACKED. Observed at scrape
  * time: each gauge runs the query once per scrape.
  */
object BacklogGauges {

  val openStates: List[FulfilmentState] =
    List(
      FulfilmentState.Pending,
      FulfilmentState.Picking,
      FulfilmentState.Packed
    )

  def register[F[_]: Monad: Clock](meter: Meter[F])(
      query: F[List[BacklogState]]
  ): Resource[F, Unit] = {
    // Every open state is reported, zero when empty, so a drained backlog
    // reads 0 instead of a stale last value.
    val complete: F[List[BacklogState]] = query.map { rows =>
      openStates.map(s =>
        rows.find(_.state == s).getOrElse(BacklogState(s, 0, None))
      )
    }
    meter
      .observableGauge[Long]("fulfilment.backlog.size")
      .createWithCallback(cb =>
        complete.flatMap(
          _.traverse_(b => cb.record(b.size, Attribute("state", b.state.wire)))
        )
      )
      .flatMap(_ =>
        meter
          .observableGauge[Double]("fulfilment.backlog.oldest_age")
          .withUnit("s")
          .createWithCallback(cb =>
            (complete, Clock[F].realTimeInstant).flatMapN { (rows, now) =>
              rows.traverse_(b =>
                cb.record(
                  b.oldestEnteredAt.fold(0.0)(at =>
                    math.max(0L, now.toEpochMilli - at.toEpochMilli) / 1000.0
                  ),
                  Attribute("state", b.state.wire)
                )
              )
            }
          )
      )
      .void
  }

  /** The in-memory store's view, for tests. */
  def fromTasks(tasks: List[FulfilmentTask]): List[BacklogState] =
    openStates.flatMap { s =>
      val inState = tasks.filter(_.state == s)
      Option.when(inState.nonEmpty)(
        BacklogState(s, inState.size.toLong, inState.map(_.updatedAt).minOption)
      )
    }

  /** The Postgres query, on its own one-connection pool so scrapes never
    * compete with operator traffic for the store's connections.
    */
  def postgresQuery[F[_]: Async: Console: Network](
      config: PostgresConfig
  ): Resource[F, F[List[BacklogState]]] = {
    import org.typelevel.otel4s.trace.Tracer.Implicits.noop
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    val query = sql"""
      SELECT state, count(*), min(updated_at) FROM "fulfilment_task"
      WHERE state IN ('PENDING', 'PICKING', 'PACKED')
      GROUP BY state
    """.query(text *: int8 *: timestamptz.opt)
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .pooled(max = 1)
      .map { pool =>
        pool
          .use(_.execute(query))
          .map(_.flatMap { case (state, n, oldest) =>
            FulfilmentState
              .fromString(state)
              .map(BacklogState(_, n, oldest.map(_.toInstant)))
          })
      }
  }
}
