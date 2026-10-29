package fulfilmentservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.parser.decode
import fulfilmentservice.domain.OrderId
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter

/** Consumes `PaymentAuthorized` on `payment-events` (US-17.1): looks the order
  * up, snapshots its items + shipping address, and creates a PENDING fulfilment
  * task (idempotent per orderId). Malformed envelopes/payloads are logged and
  * dropped; a transient order-service failure is a processing error and goes
  * through the DLQ retry path.
  */
object PaymentAuthorizedConsumer {

  val topic: String = "payment-events"

  private val eventType: String = "PaymentAuthorized"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("fulfilment-service-payment-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  def run[F[_]: Async](
      config: KafkaConfig,
      store: FulfilmentStore[F],
      orders: OrderClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        def handleEvent(event: PaymentAuthorizedEvent): F[Unit] =
          OrderId.parse(event.orderId) match {
            case Left(error) =>
              logger.error(
                Map("order_id" -> event.orderId, "error" -> error)
              )("Invalid orderId in PaymentAuthorized payload")
            case Right(orderId) =>
              orders.get(orderId).flatMap {
                case None =>
                  logger.warn(
                    Map("order_id" -> OrderId.render(orderId))
                  )("PaymentAuthorized for an unknown order - skipping")
                case Some((items, address)) =>
                  store
                    .createTask(orderId, items, address)
                    .flatMap(task =>
                      logger.info(
                        Map(
                          "order_id" -> OrderId.render(orderId),
                          "state" -> task.state.wire
                        )
                      )("Fulfilment task ready")
                    )
              }
          }

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
                    deadLetter.guard(topic, key, raw)(handleEvent(event))
                }
            }
        }
        handled *> committable.offset.commit
      }
}
