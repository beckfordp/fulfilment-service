package fulfilmentservice

import fulfilmentservice.domain.{
  FulfilmentCommand,
  FulfilmentFailure,
  FulfilmentState
}

import cats.effect.IO
import fulfilmentservice.domain.OrderId
import munit.CatsEffectSuite
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}

class HealthRoutesSuite extends CatsEffectSuite {

  private def store(ready: Boolean): FulfilmentStore[IO] =
    new FulfilmentStore[IO] {
      def ping: IO[Boolean] = IO.pure(ready)
      def createTask(
          orderId: OrderId,
          items: List[FulfilmentItem],
          shippingAddress: Address
      ): IO[FulfilmentTask] = IO.raiseError(new NotImplementedError())
      def get(orderId: OrderId): IO[Option[FulfilmentTask]] = IO.pure(None)
      def list(
          state: Option[FulfilmentState]
      ): IO[List[FulfilmentTask]] = IO.pure(Nil)
      def transition(
          orderId: OrderId,
          command: FulfilmentCommand
      ): IO[Either[FulfilmentFailure, FulfilmentTask]] =
        IO.raiseError(new NotImplementedError())
      def flagReconciliation(
          orderId: OrderId,
          reason: String
      ): IO[Boolean] = IO.pure(false)
      def reconciliationReason(orderId: OrderId): IO[Option[String]] =
        IO.pure(None)
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
