package fulfilmentservice

import cats.effect.{Async, Concurrent}
import cats.syntax.all._
import io.circe.Codec
import fulfilmentservice.domain.OrderId
import io.circe.generic.semiauto.deriveCodec
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.{Method, Request, Status, Uri}
import purerest.resilience.Resilience

/** What order-service says about an order, keeping "not found" apart from "has
  * no shipping address" (scala-fp.md law 4).
  */
enum OrderLookup {
  case Found(items: List[FulfilmentItem], shippingAddress: Address)
  case NotFound
  case NoShippingAddress
}

/** Snapshot taken from order-service when a PaymentAuthorized event arrives.
  * Non-200/404 responses raise (transient: the consumer's DLQ retry path).
  */
trait OrderClient[F[_]] {
  def get(orderId: OrderId): F[OrderLookup]
}

object OrderClient {

  private final case class OrderItem(
      id: String,
      sku: String,
      productName: String,
      unitPriceCents: Int,
      quantity: Int
  )

  private object OrderItem {
    implicit val codec: Codec[OrderItem] = deriveCodec
  }

  private final case class OrderBody(
      id: String,
      items: List[OrderItem],
      shippingAddress: Option[Address]
  )

  private object OrderBody {
    implicit val codec: Codec[OrderBody] = deriveCodec
  }

  def resilient[F[_]: Async](
      httpClient: Client[F],
      config: OrderClientConfig,
      telemetry: Telemetry[F]
  ): OrderClient[F] =
    apply(
      Resilience.middleware[F](config.resilience)(telemetry.logger)(
        telemetry.meter
      )(httpClient),
      config.baseUrl
    )

  def apply[F[_]: Concurrent](
      client: Client[F],
      baseUri: Uri
  ): OrderClient[F] =
    new OrderClient[F] {
      def get(orderId: OrderId): F[OrderLookup] =
        client
          .run(
            Request[F](Method.GET, baseUri / "orders" / OrderId.render(orderId))
          )
          .use { response =>
            response.status match {
              case Status.Ok =>
                response.as[OrderBody].map { body =>
                  body.shippingAddress match {
                    case None          => OrderLookup.NoShippingAddress
                    case Some(address) =>
                      OrderLookup.Found(
                        body.items.map(item =>
                          FulfilmentItem(
                            item.sku,
                            item.productName,
                            item.quantity,
                            item.unitPriceCents
                          )
                        ),
                        address
                      )
                  }
                }
              case Status.NotFound =>
                Concurrent[F].pure(OrderLookup.NotFound)
              case other =>
                Concurrent[F].raiseError(
                  new RuntimeException(
                    s"order-service GET /orders/$orderId returned ${other.code}"
                  )
                )
            }
          }
    }
}
