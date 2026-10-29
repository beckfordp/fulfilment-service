package fulfilmentservice

import cats.effect.IO
import fulfilmentservice.domain.OrderId
import munit.CatsEffectSuite

import java.util.UUID
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{HttpRoutes, Method, Request, Response, Status, Uri}
import org.typelevel.log4cats.noop.NoOpLogger

class FulfilmentRoutesSuite extends CatsEffectSuite {

  private val item = FulfilmentItem("sku-1", "Watch", 1, 1000)
  private val address = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private final case class Fixture(
      store: FulfilmentStore[IO],
      outbox: cats.effect.Ref[IO, List[(String, EventEnvelope)]]
  ) {
    def routes(simulation: Boolean = true): HttpRoutes[IO] =
      FulfilmentRoutes.routes[IO](store, simulation, NoOpLogger[IO])

    def id(label: String): OrderId =
      OrderId.fromUuid(UUID.nameUUIDFromBytes(label.getBytes("UTF-8")))

    def render(label: String): String = OrderId.render(id(label))

    def path(label: String, action: String): String =
      s"/fulfilment-tasks/${render(label)}/$action"

    def seed(label: String): IO[Unit] =
      store.createTask(id(label), List(item), address).void

    def run(
        routes: HttpRoutes[IO],
        method: Method,
        path: String,
        body: Option[io.circe.Json] = None
    ): IO[Response[IO]] =
      body match {
        case Some(json) =>
          routes.orNotFound.run(
            Request[IO](method, Uri.unsafeFromString(path)).withEntity(json)
          )
        case None =>
          routes.orNotFound.run(Request[IO](method, Uri.unsafeFromString(path)))
      }
  }

  private val fixture: IO[Fixture] =
    FulfilmentStore
      .inMemoryWithOutbox[IO]
      .map { case (store, outbox) => Fixture(store, outbox) }

  private def json(body: String): io.circe.Json =
    io.circe.parser.parse(body).toOption.get

  test("GET /fulfilment-tasks returns the queue and filters by state") {
    for {
      f <- fixture
      _ <- f.seed("order-1")
      _ <- f.seed("order-2")
      _ <- f.store.startPicking(f.id("order-2"))
      all <- f.run(f.routes(), Method.GET, "/fulfilment-tasks")
      pending <- f.run(
        f.routes(),
        Method.GET,
        "/fulfilment-tasks?state=PENDING"
      )
      pendingBody <- pending.as[List[TaskResponse]]
      allBody <- all.as[List[TaskResponse]]
    } yield {
      assertEquals(all.status, Status.Ok)
      assertEquals(
        allBody.map(_.orderId).sorted,
        List(f.render("order-1"), f.render("order-2")).sorted
      )
      assertEquals(pendingBody.map(_.orderId), List(f.render("order-1")))
    }
  }

  test("an invalid state filter is 400") {
    for {
      f <- fixture
      response <- f.run(
        f.routes(),
        Method.GET,
        "/fulfilment-tasks?state=NOPE"
      )
    } yield assertEquals(response.status, Status.BadRequest)
  }

  test("start-picking, pack and dispatch drive the task to DISPATCHED") {
    for {
      f <- fixture
      _ <- f.seed("order-1")
      picking <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-1", "start-picking")
      )
      pickingBody <- picking.as[TaskResponse]
      _ <- f.run(f.routes(), Method.POST, f.path("order-1", "pack"))
      dispatched <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-1", "dispatch"),
        Some(json("""{"trackingNo":"GLN-9","carrier":"DHL"}"""))
      )
      dispatchedBody <- dispatched.as[TaskResponse]
      events <- f.outbox.get
    } yield {
      assertEquals(pickingBody.state, "PICKING")
      assertEquals(dispatched.status, Status.Ok)
      assertEquals(dispatchedBody.state, "DISPATCHED")
      assertEquals(dispatchedBody.trackingNo, Some("GLN-9"))
      assertEquals(events.map(_._2.eventType), List("DispatchConfirmed"))
    }
  }

  test("dispatch validates its body") {
    for {
      f <- fixture
      _ <- f.seed("order-1")
      _ <- f.store.startPicking(f.id("order-1"))
      _ <- f.store.pack(f.id("order-1"))
      response <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-1", "dispatch"),
        Some(json("""{"trackingNo":" ","carrier":""}"""))
      )
    } yield assertEquals(response.status, Status.BadRequest)
  }

  test("fail requires a reason and emits DispatchFailed") {
    for {
      f <- fixture
      _ <- f.seed("order-1")
      blank <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-1", "fail"),
        Some(json("""{"reason":" "}"""))
      )
      failed <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-1", "fail"),
        Some(json("""{"reason":"damaged"}"""))
      )
      body <- failed.as[TaskResponse]
      events <- f.outbox.get
    } yield {
      assertEquals(blank.status, Status.BadRequest)
      assertEquals(body.state, "DISPATCH_FAILED")
      assertEquals(events.map(_._2.eventType), List("DispatchFailed"))
    }
  }

  test("unknown order is 404 and an invalid transition is 409") {
    for {
      f <- fixture
      missing <- f.run(
        f.routes(),
        Method.POST,
        f.path("unknown", "start-picking")
      )
      _ <- f.seed("order-1")
      invalid <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-1", "pack")
      )
    } yield {
      assertEquals(missing.status, Status.NotFound)
      assertEquals(invalid.status, Status.Conflict)
    }
  }

  test("simulate-next is not registered when simulation is disabled") {
    for {
      f <- fixture
      _ <- f.seed("order-1")
      response <- f.run(
        f.routes(simulation = false),
        Method.POST,
        f.path("order-1", "simulate-next"),
        Some(json("{}"))
      )
    } yield assertEquals(response.status, Status.NotFound)
  }

  test(
    "simulate-next advances one step, generates tracking, and can force failure"
  ) {
    for {
      f <- fixture
      _ <- f.seed("order-1")
      _ <- f.seed("order-2")
      first <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-1", "simulate-next"),
        Some(json("{}"))
      )
      firstBody <- first.as[TaskResponse]
      _ <- f.store.pack(f.id("order-1"))
      dispatched <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-1", "simulate-next"),
        Some(json("{}"))
      )
      dispatchedBody <- dispatched.as[TaskResponse]
      failed <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-2", "simulate-next"),
        Some(json("""{"forceFailure":true,"reason":"simulated"}"""))
      )
      failedBody <- failed.as[TaskResponse]
    } yield {
      assertEquals(firstBody.state, "PICKING")
      assertEquals(dispatchedBody.state, "DISPATCHED")
      assert(dispatchedBody.trackingNo.exists(_.startsWith("GLN-")))
      assertEquals(failedBody.state, "DISPATCH_FAILED")
      assertEquals(failedBody.failureReason, Some("simulated"))
    }
  }

  test("simulate-next on a terminal task is 409") {
    for {
      f <- fixture
      _ <- f.seed("order-1")
      _ <- f.store.fail(f.id("order-1"), "gone")
      response <- f.run(
        f.routes(),
        Method.POST,
        f.path("order-1", "simulate-next"),
        Some(json("{}"))
      )
    } yield assertEquals(response.status, Status.Conflict)
  }
}
