package fulfilmentservice

import cats.effect.Async
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.{Method, Request, Status, Uri}
import purerest.resilience.Resilience

/** Snapshot taken from order-service when a PaymentAuthorized event arrives. */
trait OrderClient[F[_]] {
  def get(orderId: String): F[Option[(List[FulfilmentItem], Address)]]
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
      Uri.unsafeFromString(config.baseUrl)
    )

  def apply[F[_]: Async](
      client: Client[F],
      baseUri: Uri
  ): OrderClient[F] =
    new OrderClient[F] {
      def get(
          orderId: String
      ): F[Option[(List[FulfilmentItem], Address)]] =
        client.run(Request[F](Method.GET, baseUri / "orders" / orderId)).use {
          response =>
            response.status match {
              case Status.Ok =>
                response.as[OrderBody].map { body =>
                  body.shippingAddress.map { address =>
                    (
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
              case Status.NotFound => Async[F].pure(None)
              case other           =>
                Async[F].raiseError(
                  new RuntimeException(
                    s"order-service GET /orders/$orderId returned ${other.code}"
                  )
                )
            }
        }
    }
}
