package fulfilmentservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import purerest.docs.Docs
import sttp.tapir.EndpointOutput
import sttp.tapir.server.ServerEndpoint

class FulfilmentDocsSuite extends CatsEffectSuite {

  /** Every fixed status an output can produce (tapir's own traversal is
    * internal).
    */
  private def statuses(output: EndpointOutput[?]): List[Int] =
    output match {
      case EndpointOutput.FixedStatusCode(code, _, _) => List(code.code)
      case EndpointOutput.OneOf(variants, _)          =>
        variants.flatMap(v => statuses(v.output))
      case EndpointOutput.Pair(left, right, _, _) =>
        statuses(left) ++ statuses(right)
      case EndpointOutput.MappedPair(pair, _) => statuses(pair)
      case _                                  => Nil
    }

  private def errorStatuses(endpoint: ServerEndpoint[Any, IO]): List[Int] =
    statuses(endpoint.endpoint.errorOutput).sorted

  /** The same endpoint list `AppRoutes` passes to `Docs.routes` (simulation on,
    * so `simulate-next` is included).
    */
  private def servedEndpoints(
      store: FulfilmentStore[IO]
  ): List[ServerEndpoint[Any, IO]] =
    FulfilmentRoutes.serverEndpoints[IO](
      store,
      simulationEnabled = true,
      NoOpLogger[IO]
    ) ++ List(
      ReconciliationRoutes.listServerEndpoint[IO](store, NoOpLogger[IO]),
      ReconciliationRoutes.resolveServerEndpoint[IO](
        store,
        OperatorAuth.disabled,
        NoOpLogger[IO]
      ),
      HealthRoutes.healthServerEndpoint[IO],
      HealthRoutes.readyServerEndpoint[IO](store)
    )

  test("each endpoint documents only the errors it can return") {
    for {
      store <- FulfilmentStore.inMemory[IO]
    } yield {
      val served = servedEndpoints(store)
      assertEquals(
        served.map(errorStatuses),
        List(
          List(400), // GET /fulfilment-tasks (unknown state)
          List(404, 409), // start-picking
          List(404, 409), // pack
          List(400, 404, 409), // dispatch (blank fields)
          List(400, 404, 409), // fail (blank reason)
          List(404, 409), // simulate-next (terminal)
          List(400), // GET /reconciliations
          List(400, 401, 404), // PATCH /reconciliations/{orderId}
          Nil, // GET /health
          List(503) // GET /health/ready
        )
      )
    }
  }

  test("every endpoint is documented") {
    for {
      store <- FulfilmentStore.inMemory[IO]
    } yield {
      val gaps =
        Docs.undocumented(servedEndpoints(store).map(_.endpoint)).map(_.render)
      assertEquals(gaps, Nil)
    }
  }
}
