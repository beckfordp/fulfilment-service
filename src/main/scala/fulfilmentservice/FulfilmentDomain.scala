package fulfilmentservice

import fulfilmentservice.domain.OrderId
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

import java.time.Instant

final case class FulfilmentItem(
    sku: String,
    productName: String,
    quantity: Int,
    unitPriceCents: Int
)

object FulfilmentItem {
  implicit val codec: Codec[FulfilmentItem] = deriveCodec
  implicit val schema: Schema[FulfilmentItem] =
    Schema.derived[FulfilmentItem].copy(name = None)
}

final case class Address(
    line1: String,
    line2: Option[String],
    city: String,
    postalCode: String,
    country: String
)

object Address {
  implicit val codec: Codec[Address] = deriveCodec
  implicit val schema: Schema[Address] = Schema.derived
}

enum FulfilmentState(val wire: String) {
  case Pending extends FulfilmentState("PENDING")
  case Picking extends FulfilmentState("PICKING")
  case Packed extends FulfilmentState("PACKED")
  case Dispatched extends FulfilmentState("DISPATCHED")
  case DispatchFailed extends FulfilmentState("DISPATCH_FAILED")
}

object FulfilmentState {
  def fromString(value: String): Option[FulfilmentState] =
    values.find(_.wire == value)

  def isTerminal(state: FulfilmentState): Boolean =
    state == Dispatched || state == DispatchFailed
}

final case class FulfilmentTask(
    orderId: OrderId,
    state: FulfilmentState,
    items: List[FulfilmentItem],
    shippingAddress: Address,
    trackingNo: Option[String],
    carrier: Option[String],
    failureReason: Option[String],
    createdAt: Instant,
    updatedAt: Instant
)

sealed trait FulfilmentFailure

object FulfilmentFailure {
  case object NotFound extends FulfilmentFailure
  final case class InvalidTransition(message: String) extends FulfilmentFailure
  final case class InvalidRequest(message: String) extends FulfilmentFailure
}
