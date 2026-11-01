package fulfilmentservice

import cats.effect.Sync
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import fulfilmentservice.domain.{FulfilmentCommand, OrderId}
import io.circe.syntax._
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

import java.time.Instant
import java.util.UUID

/** Shared Kafka envelope (ADR 0009). */
final case class EventEnvelope(
    eventId: String,
    eventType: String,
    timestamp: Instant,
    source: String,
    correlationId: String,
    payload: io.circe.Json
)

object EventEnvelope {
  implicit val codec: Codec[EventEnvelope] = deriveCodec
}

/** `DispatchConfirmed` payload (US-17.3): a full item snapshot, so consumers
  * need no order lookup.
  *
  * Descriptions/examples are copied from the producer's artifact in gluon's
  * `registry/` (TD-19); nothing checks they stay current.
  */
@description(
  "Order dispatched: pick/pack/dispatch succeeded. Carries a full item snapshot so consumers need no order lookup."
)
final case class DispatchConfirmedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Carrier tracking number.")
    @encodedExample("TRK-123456789")
    trackingNo: String,
    @description("Items dispatched. Always sent.")
    items: List[FulfilmentItem]
)

object DispatchConfirmedEvent {
  implicit val codec: Codec[DispatchConfirmedEvent] = deriveCodec
  implicit val schema: Schema[DispatchConfirmedEvent] = Schema.derived
}

/** `DispatchFailed` payload (US-18.1). */
@description(
  "Dispatch failed: pick/pack/dispatch hit an error. payment-service voids the hold; order-service marks the order failed."
)
final case class DispatchFailedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Why dispatch failed.")
    @encodedExample("carrier_unavailable")
    reason: String
)

object DispatchFailedEvent {
  implicit val codec: Codec[DispatchFailedEvent] = deriveCodec
  implicit val schema: Schema[DispatchFailedEvent] = Schema.derived
}

/** The event a `PaymentAuthorized` consumer reads (payment-service producer).
  */
@description(
  "Funds held: POST /payments/authorize succeeded. Consumed by fulfilment-service to start pick/pack."
)
final case class PaymentAuthorizedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Payment provider's authorization reference.")
    @encodedExample("auth-7f3a9c")
    authId: String,
    @description("When the hold lapses if never captured (ISO-8601 instant).")
    @encodedExample("2026-11-07T10:15:30Z")
    expiresAt: Instant,
    @description("When the authorization was made (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant
)

object PaymentAuthorizedEvent {
  implicit val codec: Codec[PaymentAuthorizedEvent] = deriveCodec
  implicit val schema: Schema[PaymentAuthorizedEvent] = Schema.derived
}

object FulfilmentEvents {

  private val source = "fulfilment-service"

  private def envelope[F[_]: Sync](
      eventType: String,
      orderId: String,
      payload: io.circe.Json
  ): F[EventEnvelope] =
    for {
      eventId <- Sync[F].delay(UUID.randomUUID().toString)
      timestamp <- Sync[F].realTimeInstant
    } yield EventEnvelope(
      eventId = eventId,
      eventType = eventType,
      timestamp = timestamp,
      source = source,
      correlationId = orderId,
      payload = payload
    )

  def dispatchConfirmed[F[_]: Sync](
      orderId: OrderId,
      trackingNo: String,
      items: List[FulfilmentItem]
  ): F[EventEnvelope] = {
    val rendered = OrderId.render(orderId)
    envelope[F](
      "DispatchConfirmed",
      rendered,
      DispatchConfirmedEvent(rendered, trackingNo, items).asJson
    )
  }

  def dispatchFailed[F[_]: Sync](
      orderId: OrderId,
      reason: String
  ): F[EventEnvelope] = {
    val rendered = OrderId.render(orderId)
    envelope[F](
      "DispatchFailed",
      rendered,
      DispatchFailedEvent(rendered, reason).asJson
    )
  }

  /** The outbox event a successful `command` publishes, defined once for both
    * store interpreters: `Dispatch` -> DispatchConfirmed (item snapshot),
    * `Fail` -> DispatchFailed, picking/packing -> none.
    */
  def forCommand[F[_]: Sync](
      orderId: OrderId,
      task: FulfilmentTask,
      command: FulfilmentCommand
  ): F[Option[EventEnvelope]] =
    command match {
      case FulfilmentCommand.Dispatch(trackingNo, _) =>
        dispatchConfirmed[F](orderId, trackingNo, task.items).map(Some(_))
      case FulfilmentCommand.Fail(reason) =>
        dispatchFailed[F](orderId, reason).map(Some(_))
      case FulfilmentCommand.StartPicking | FulfilmentCommand.Pack =>
        Sync[F].pure(None)
    }
}
