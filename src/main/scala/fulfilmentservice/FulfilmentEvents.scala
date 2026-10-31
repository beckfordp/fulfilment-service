package fulfilmentservice

import cats.effect.Sync
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import fulfilmentservice.domain.OrderId
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
final case class PaymentAuthorizedEvent(
    orderId: String,
    authId: String,
    expiresAt: Instant,
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
}
