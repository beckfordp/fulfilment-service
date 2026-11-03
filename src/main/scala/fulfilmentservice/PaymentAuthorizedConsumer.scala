package fulfilmentservice

import cats.Monad
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.parser.decode
import fulfilmentservice.domain.OrderId
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.{DeadLetter, EventAge}

/** Consumes `PaymentAuthorized` on `payment-events` (US-17.1): looks the order
  * up, snapshots its items + shipping address, and creates a PENDING fulfilment
  * task (idempotent per orderId). Malformed envelopes/payloads are logged and
  * dropped; a transient order-service failure is a processing error and goes
  * through the DLQ retry path.
  */
object PaymentAuthorizedConsumer {

  val topic: String = "payment-events"

  val groupId: String = "fulfilment-service-payment-events"

  private val eventType: String = "PaymentAuthorized"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(groupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  /** One decoded event: parse the orderId, look the order up, create the task
    * (idempotent). Invalid ids and unusable orders are logged and dropped; an
    * order-service error raises (DLQ retry).
    */
  def handle[F[_]: Monad](
      store: FulfilmentStore[F],
      orders: OrderClient[F],
      logger: StructuredLogger[F]
  )(event: PaymentAuthorizedEvent): F[Unit] =
    OrderId.parse(event.orderId) match {
      case Left(error) =>
        logger.error(
          Map("order_id" -> event.orderId, "error" -> error)
        )("Invalid orderId in PaymentAuthorized payload")
      case Right(orderId) =>
        val context = Map("order_id" -> OrderId.render(orderId))
        orders.get(orderId).flatMap {
          case OrderLookup.NotFound =>
            logger.warn(context)(
              "PaymentAuthorized for an unknown order - skipping"
            )
          case OrderLookup.NoShippingAddress =>
            logger.error(context)(
              "PaymentAuthorized for an order with no shipping address - skipping"
            )
          case OrderLookup.Found(items, address) =>
            store
              .createTask(orderId, items, address)
              .flatMap(task =>
                logger.info(context + ("state" -> task.state.wire))(
                  "Fulfilment task ready"
                )
              )
        }
    }

  def run[F[_]: Async](
      config: KafkaConfig,
      store: FulfilmentStore[F],
      orders: OrderClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String],
      eventAge: EventAge[F]
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
        val key = committable.record.key.getOrElse("")

        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger
              .error(Map.empty)(
                "Received payment-events record with a null value - skipping"
              )
              .as(())
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger
                  .error(
                    Map("topic" -> topic, "bytes" -> raw.length.toString),
                    error
                  )(
                    "Failed to decode payment-events envelope"
                  )
                  .as(())
              case Right(envelope) if envelope.eventType != eventType =>
                logger.info(
                  Map("topic" -> topic, "event_type" -> envelope.eventType)
                )("Ignoring unrelated payment-events event type")
              case Right(envelope) =>
                envelope.payload.as[PaymentAuthorizedEvent] match {
                  case Left(error) =>
                    logger
                      .error(
                        Map("topic" -> topic, "bytes" -> raw.length.toString),
                        error
                      )(
                        "Failed to decode PaymentAuthorized payload"
                      )
                      .as(())
                  case Right(event) =>
                    deadLetter.guard(topic, key, raw)(
                      handle[F](store, orders, logger)(event)
                    )
                }
            }
        }
        handled *> committable.offset.commit
      }
}
