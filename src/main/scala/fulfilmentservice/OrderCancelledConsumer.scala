package fulfilmentservice

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import fulfilmentservice.domain.{
  FulfilmentCommand,
  FulfilmentFailure,
  FulfilmentState,
  OrderId
}
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.trace.Tracer
import purekafka.consumer.{DeadLetter, EventAge}
import purekafka.tracing.KafkaTracing
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

import java.time.Instant

/** Local mirror of order-service's `OrderCancelled` payload (US-9.2). `reason`
  * is optional so old reason-less messages decode; the abort does not depend on
  * it.
  */
@description(
  "Order cancelled: a customer cancel or a consumed PaymentAuthorizationExpired moved the order to cancelled."
)
final case class OrderCancelledEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Customer who placed the order.")
    @encodedExample("7c9e6679-7425-40de-944b-e07fc1f90ae7")
    customerId: String,
    @description("When the order was cancelled (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant,
    @description(
      "Cancellation trigger: customer | auth_expired | stale | operator."
    )
    @encodedExample("customer")
    reason: Option[String] = None
)

object OrderCancelledEvent {
  implicit val codec: Codec[OrderCancelledEvent] = deriveCodec
  implicit val schema: Schema[OrderCancelledEvent] = Schema.derived
}

/** US-9.5: consumes order-service's shared `OrderCancelled` event and aborts a
  * task that has not dispatched. `PENDING`/`PICKING`/`PACKED -> CANCELLED`;
  * `DISPATCHED`, `DISPATCH_FAILED`, an already-`CANCELLED` task and an unknown
  * order are logged no-ops. Cancelling publishes nothing.
  */
object OrderCancelledConsumer {

  val topic: String = "order-events"

  val groupId: String = "fulfilment-service-order-events"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(groupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private def handleCancelled[F[_]: MonadThrow](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F]
  )(orderId: OrderId, event: OrderCancelledEvent): F[Unit] = {
    val context = Map(
      "order_id" -> event.orderId,
      "reason" -> event.reason.getOrElse("-")
    )
    store.transition(orderId, FulfilmentCommand.Cancel).flatMap {
      case Right(task) =>
        logger.info(context + ("state" -> task.state.wire))(
          "Fulfilment task aborted on order cancellation"
        )
      case Left(FulfilmentFailure.NotFound) =>
        logger.warn(context)(
          "OrderCancelled for an unknown fulfilment task - skipping"
        )
      case Left(
            FulfilmentFailure.InvalidTransition(
              message,
              Some(FulfilmentState.Dispatched)
            )
          ) =>
        logger.warn(context + ("detail" -> message))(
          "OrderCancelled after dispatch - reconciliation flagged"
        ) *> store.flagReconciliation(orderId, "cancel_after_dispatch").void
      case Left(FulfilmentFailure.InvalidTransition(message, _)) =>
        logger.info(context + ("detail" -> message))(
          "OrderCancelled for a task that already dispatched or was cancelled - skipping"
        )
      case Left(FulfilmentFailure.InvalidRequest(message)) =>
        MonadThrow[F].raiseError(
          new IllegalStateException(
            s"Cancel produced an invalid request: $message"
          )
        )
    }
  }

  /** One `order-events` record, minus the offset commit - exposed for
    * broker-free handler tests.
    */
  def handle[F[_]: MonadThrow](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  )(key: String, value: Option[String]): F[Unit] = {
    def deadLetterRaw(raw: String, error: Throwable): F[Unit] =
      deadLetter.guard(topic, key, raw)(MonadThrow[F].raiseError(error))

    value match {
      case None =>
        logger.error(Map.empty)(
          "Received order-events record with a null value - skipping"
        )
      case Some(raw) =>
        decode[EventEnvelope](raw) match {
          case Left(error) =>
            logger.error(
              Map("topic" -> topic, "bytes" -> raw.length.toString),
              error
            )("Failed to decode order-events envelope") *>
              deadLetterRaw(raw, error)
          case Right(envelope) =>
            envelope.eventType match {
              case "OrderCancelled" =>
                envelope.payload.as[OrderCancelledEvent] match {
                  case Left(error) =>
                    logger.error(
                      Map("topic" -> topic, "bytes" -> raw.length.toString),
                      error
                    )("Failed to decode OrderCancelled payload") *>
                      deadLetterRaw(raw, error)
                  case Right(event) =>
                    OrderId.parse(event.orderId) match {
                      case Left(error) =>
                        deadLetterRaw(raw, new IllegalArgumentException(error))
                      case Right(orderId) =>
                        deadLetter.guard(topic, key, raw)(
                          handleCancelled(store, logger)(orderId, event)
                        )
                    }
                }
              case other =>
                logger.info(
                  Map("topic" -> topic, "event_type" -> other)
                )("Ignoring unrelated order-events event type")
            }
        }
    }
  }

  def run[F[_]: Async](
      config: KafkaConfig,
      store: FulfilmentStore[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String],
      eventAge: EventAge[F],
      tracer: Tracer[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalTap(committable =>
        committable.record.timestamp.createTime.traverse_(
          eventAge.record(topic, groupId, _)
        )
      )
      .evalMap { committable =>
        KafkaTracing.traced(tracer)(
          committable.record.headers,
          s"$topic process",
          topic
        )(
          handle(store, logger, deadLetter)(
            committable.record.key.getOrElse(""),
            committable.record.value
          )
        ) *> committable.offset.commit
      }
}
