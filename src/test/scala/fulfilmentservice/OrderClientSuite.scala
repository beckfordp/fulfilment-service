package fulfilmentservice

import cats.effect.IO
import fulfilmentservice.domain.OrderId
import munit.CatsEffectSuite

import java.util.UUID
import org.http4s.{HttpApp, Response, Status, Uri}
import org.http4s.dsl.io._
import org.http4s.client.Client

class OrderClientSuite extends CatsEffectSuite {

  private val orderId = OrderId.fromUuid(UUID.randomUUID())

  private val base = Uri.unsafeFromString("http://order.test")

  private def client(app: HttpApp[IO]): OrderClient[IO] =
    OrderClient[IO](Client.fromHttpApp(app), base)

  private val body =
    """{"id":"order-1","customerId":"c1","totalCents":1000,"status":"authorized",
      "items":[{"id":"i1","sku":"sku-1","productName":"Watch","unitPriceCents":1000,"quantity":2}],
      "createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z",
      "shippingAddress":{"line1":"1 High St","line2":null,"city":"London","postalCode":"SW1A 1AA","country":"GB"}}"""

  test("GET /orders/{id} returns the item + address snapshot") {
    val app = HttpApp[IO] { request =>
      if (
        request.uri.path.renderString == s"/orders/${OrderId.render(orderId)}"
      )
        IO.pure(Response[IO](Status.Ok).withEntity(body))
      else IO.pure(Response[IO](Status.NotFound))
    }

    client(app).get(orderId).map { result =>
      assertEquals(
        result.map(_._1),
        Some(List(FulfilmentItem("sku-1", "Watch", 2, 1000)))
      )
      assertEquals(result.map(_._2.line1), Some("1 High St"))
    }
  }

  test("a 404 becomes None") {
    val app = HttpApp[IO](_ => IO.pure(Response[IO](Status.NotFound)))

    client(app)
      .get(OrderId.fromUuid(UUID.randomUUID()))
      .map(result => assertEquals(result, None))
  }

  test("a transient error raises") {
    val app = HttpApp[IO](_ => IO.pure(Response[IO](Status.ServiceUnavailable)))

    client(app).get(orderId).attempt.map { result =>
      assert(result.isLeft)
    }
  }
}
