package fulfilmentservice

import cats.syntax.all._
import fulfilmentservice.domain.{
  FulfilmentCommand,
  FulfilmentFailure,
  FulfilmentLifecycle,
  FulfilmentState,
  OrderId
}
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

import java.time.Instant

final case class FulfilmentItem(
    @description("Product SKU.")
    @encodedExample("SKU-0042")
    sku: String,
    @description("Product name at dispatch time.")
    @encodedExample("Espresso cup")
    productName: String,
    @description("Units dispatched.")
    @encodedExample(2)
    quantity: Int,
    @description("Unit price, in cents.")
    @encodedExample(1250)
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

object FulfilmentTask {

  /** Applies `command` to `task` via the pure lifecycle, stamping `now` as
    * `updatedAt`. Shared by both store interpreters.
    */
  def applyCommand(
      task: FulfilmentTask,
      command: FulfilmentCommand,
      now: Instant
  ): Either[FulfilmentFailure, FulfilmentTask] =
    FulfilmentLifecycle
      .transition(task.state, command)
      .leftMap(_ =>
        FulfilmentFailure.InvalidTransition(
          s"Cannot transition order ${OrderId.render(task.orderId)} from ${task.state.wire}"
        )
      )
      .map { next =>
        val moved = task.copy(state = next, updatedAt = now)
        command match {
          case FulfilmentCommand.Dispatch(trackingNo, carrier) =>
            moved.copy(trackingNo = Some(trackingNo), carrier = Some(carrier))
          case FulfilmentCommand.Fail(reason) =>
            moved.copy(failureReason = Some(reason))
          case FulfilmentCommand.StartPicking | FulfilmentCommand.Pack => moved
        }
      }
}
