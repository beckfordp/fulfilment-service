package fulfilmentservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}

class HealthRoutesSuite extends CatsEffectSuite {

  private def store(ready: Boolean): FulfilmentStore[IO] =
    new FulfilmentStore[IO] {
      def ping: IO[Boolean] = IO.pure(ready)
      def createTask(
          orderId: String,
          items: List[FulfilmentItem],
          shippingAddress: Address
      ): IO[FulfilmentTask] = IO.raiseError(new NotImplementedError())
      def get(orderId: String): IO[Option[FulfilmentTask]] = IO.pure(None)
      def list(
          state: Option[FulfilmentState]
      ): IO[List[FulfilmentTask]] = IO.pure(Nil)
      def startPicking(
          orderId: String
      ): IO[Either[FulfilmentFailure, FulfilmentTask]] =
        IO.raiseError(new NotImplementedError())
      def pack(
          orderId: String
      ): IO[Either[FulfilmentFailure, FulfilmentTask]] =
        IO.raiseError(new NotImplementedError())
      def dispatch(
          orderId: String,
          trackingNo: String,
          carrier: String
      ): IO[Either[FulfilmentFailure, FulfilmentTask]] =
        IO.raiseError(new NotImplementedError())
      def fail(
          orderId: String,
          reason: String
      ): IO[Either[FulfilmentFailure, FulfilmentTask]] =
        IO.raiseError(new NotImplementedError())
    }

  test("GET /health returns 200") {
    val routes = HealthRoutes.routes[IO](store(ready = true))
    for {
      response <- routes.orNotFound.run(Request[IO](Method.GET, uri"/health"))
    } yield assertEquals(response.status, Status.Ok)
  }

  test("GET /health/ready returns 200 when the store is ready") {
    val routes = HealthRoutes.routes[IO](store(ready = true))
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
    } yield assertEquals(response.status, Status.Ok)
  }

  test("GET /health/ready returns 503 when the store is not ready") {
    val routes = HealthRoutes.routes[IO](store(ready = false))
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
    } yield assertEquals(response.status, Status.ServiceUnavailable)
  }
}
