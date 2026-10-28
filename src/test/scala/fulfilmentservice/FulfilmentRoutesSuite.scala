package fulfilmentservice

import cats.effect.IO
import cats.syntax.all._
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.{
  ERROR,
  INFO,
  WARN
}
import org.typelevel.ci._
import purerest.tracing.{ServerTracing, Tracing}

import scala.concurrent.duration._

class FulfilmentRoutesSuite extends CatsEffectSuite {

  private def failingStore(error: Throwable): FulfilmentStore[IO] =
    new FulfilmentStore[IO] {
      def create(/* codegen:fields:CREATE_PARAMS */): IO[Fulfilment] =
        IO.raiseError(error)
      def get(id: String): IO[Option[Fulfilment]] = IO.pure(None)
      def update(
          id: String
          /* codegen:fields:UPDATE_PARAMS */
      ): IO[Option[Fulfilment]] =
        IO.raiseError(error)
      def delete(id: String): IO[Boolean] = IO.raiseError(error)
      def list(
          limit: Int,
          offset: Int
      ): IO[(List[Fulfilment], Long)] = IO.raiseError(error)
      def ping: IO[Boolean] = IO.raiseError(error)
    }

  test("POST /fulfilments returns 201 with the created entity") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      request = Request[IO](Method.POST, uri"/fulfilments")
        .withEntity(CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */))
      response <- routes.orNotFound.run(request)
      entity <- response.as[FulfilmentResponse]
    } yield {
      assertEquals(response.status, Status.Created)
      assert(entity.id.nonEmpty)
    }
  }

  test("GET /fulfilments/{id} returns 200 with the persisted entity") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/fulfilments").withEntity(
          CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[FulfilmentResponse]
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments" / created.id)
      )
      fetched <- getResponse.as[FulfilmentResponse]
    } yield {
      assertEquals(getResponse.status, Status.Ok)
      assertEquals(fetched, created)
    }
  }

  test(
    "GET /fulfilments/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments" / "unknown-id")
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "POST /fulfilments logs a received-request line and a completed line with structured context"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = FulfilmentRoutes.routes[IO](store, testLogger)
      request = Request[IO](Method.POST, uri"/fulfilments")
        .withEntity(CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */))
      response <- routes.orNotFound.run(request)
      entity <- response.as[FulfilmentResponse]
      logged <- testLogger.logged
    } yield {
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("POST")
        ),
        s"expected a received-request INFO line with method context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("fulfilment_id").contains(entity.id)
        ),
        s"expected a completed INFO line with fulfilment_id context, got: $infos"
      )
    }
  }

  test(
    "POST /fulfilments logs an ERROR with the raised throwable when persisting the entity fails"
  ) {
    val boom = new RuntimeException("boom")
    for {
      testLogger <- IO.pure(StructuredTestingLogger.impl[IO]())
      routes = FulfilmentRoutes.routes[IO](failingStore(boom), testLogger)
      request = Request[IO](Method.POST, uri"/fulfilments")
        .withEntity(CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */))
      response <- routes.orNotFound.run(request)
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.InternalServerError)
      val errors = logged.collect { case m: ERROR => m }
      assert(
        errors.exists(m => m.throwOpt.contains(boom)),
        s"expected an ERROR line with the raised throwable, got: $errors"
      )
    }
  }

  test(
    "GET /fulfilments/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = FulfilmentRoutes.routes[IO](store, testLogger)
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/fulfilments").withEntity(
          CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[FulfilmentResponse]
      _ <- testLogger.logged // drain POST's own log lines before the GET
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments" / created.id)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(getResponse.status, Status.Ok)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("GET") &&
            m.ctx.get("fulfilment_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("fulfilment_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test(
    "GET /fulfilments/{id} logs a WARN for an unknown id"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = FulfilmentRoutes.routes[IO](store, testLogger)
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments" / "unknown-id")
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("fulfilment_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test("PATCH /fulfilments/{id} returns 200 with the updated entity") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/fulfilments").withEntity(
          CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[FulfilmentResponse]
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/fulfilments" / created.id)
          .withEntity(UpdateFulfilmentRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      updated <- patchResponse.as[FulfilmentResponse]
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      assertEquals(updated.id, created.id)
    }
  }

  test(
    "PATCH /fulfilments/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/fulfilments" / "unknown-id")
          .withEntity(UpdateFulfilmentRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "PATCH /fulfilments/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = FulfilmentRoutes.routes[IO](store, testLogger)
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/fulfilments").withEntity(
          CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[FulfilmentResponse]
      _ <- testLogger.logged // drain POST's own log lines before the PATCH
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/fulfilments" / created.id)
          .withEntity(UpdateFulfilmentRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("PATCH") &&
            m.ctx.get("fulfilment_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("fulfilment_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test("PATCH /fulfilments/{id} logs a WARN for an unknown id") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = FulfilmentRoutes.routes[IO](store, testLogger)
      response <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/fulfilments" / "unknown-id")
          .withEntity(UpdateFulfilmentRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("fulfilment_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test(
    "DELETE /fulfilments/{id} returns 204, and a subsequent GET returns 404"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/fulfilments").withEntity(
          CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[FulfilmentResponse]
      deleteResponse <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/fulfilments" / created.id)
      )
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments" / created.id)
      )
    } yield {
      assertEquals(deleteResponse.status, Status.NoContent)
      assertEquals(getResponse.status, Status.NotFound)
    }
  }

  test(
    "DELETE /fulfilments/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/fulfilments" / "unknown-id")
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "DELETE /fulfilments/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = FulfilmentRoutes.routes[IO](store, testLogger)
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/fulfilments").withEntity(
          CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[FulfilmentResponse]
      _ <- testLogger.logged // drain POST's own log lines before the DELETE
      deleteResponse <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/fulfilments" / created.id)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(deleteResponse.status, Status.NoContent)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("DELETE") &&
            m.ctx.get("fulfilment_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("fulfilment_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test("DELETE /fulfilments/{id} logs a WARN for an unknown id") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = FulfilmentRoutes.routes[IO](store, testLogger)
      response <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/fulfilments" / "unknown-id")
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("fulfilment_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test("PUT /fulfilments/{id} returns 200 with the replaced entity") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/fulfilments").withEntity(
          CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      created <- postResponse.as[FulfilmentResponse]
      putResponse <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/fulfilments" / created.id)
          .withEntity(UpdateFulfilmentRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      replaced <- putResponse.as[FulfilmentResponse]
    } yield {
      assertEquals(putResponse.status, Status.Ok)
      assertEquals(replaced.id, created.id)
    }
  }

  test(
    "PUT /fulfilments/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/fulfilments" / "unknown-id")
          .withEntity(UpdateFulfilmentRequest(/* codegen:fields:TEST_UPDATE_REQUEST_ARGS */))
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "wrapped routes (with tracing middleware) record a span for a handled request"
  ) {
    Tracing.test[IO]("fulfilment-service-test").use { testTracer =>
      for {
        store <- FulfilmentStore.inMemory[IO]
        routes = ServerTracing.middleware(testTracer.tracer)(
          FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
        )
        request = Request[IO](Method.POST, uri"/fulfilments")
          .withEntity(CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */))
        response <- routes.orNotFound.run(request)
        spans <- testTracer.finishedSpans
      } yield {
        assertEquals(response.status, Status.Created)
        assertEquals(spans.map(_.getName), List("POST /fulfilments"))
      }
    }
  }

  test(
    "GET /fulfilments with no query params returns up to 20 entities newest-first with X-Total-Count"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      first <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/fulfilments").withEntity(
          CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      firstEntity <- first.as[FulfilmentResponse]
      _ <- IO.sleep(2.millis)
      second <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/fulfilments").withEntity(
          CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
        )
      )
      secondEntity <- second.as[FulfilmentResponse]
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments")
      )
      body <- response.as[List[FulfilmentResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.map(_.id), List(secondEntity.id, firstEntity.id))
      assertEquals(
        response.headers.get(ci"X-Total-Count").map(_.head.value),
        Some("2")
      )
    }
  }

  test("GET /fulfilments defaults to a page of at most 20 entities") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      _ <- List.fill(21)(store.create(/* codegen:fields:TEST_CREATE_ARGS */)).sequence
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments")
      )
      body <- response.as[List[FulfilmentResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.size, 20)
      assertEquals(
        response.headers.get(ci"X-Total-Count").map(_.head.value),
        Some("21")
      )
    }
  }

  test("GET /fulfilments respects limit and offset") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      _ <- store.create(/* codegen:fields:TEST_CREATE_ARGS */)
      _ <- IO.sleep(2.millis)
      second <- store.create(/* codegen:fields:TEST_CREATE_ARGS */)
      _ <- IO.sleep(2.millis)
      _ <- store.create(/* codegen:fields:TEST_CREATE_ARGS */)
      response <- routes.orNotFound.run(
        Request[IO](
          Method.GET,
          uri"/fulfilments".withQueryParam("limit", 1).withQueryParam("offset", 1)
        )
      )
      body <- response.as[List[FulfilmentResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.map(_.id), List(second.id))
      assertEquals(
        response.headers.get(ci"X-Total-Count").map(_.head.value),
        Some("3")
      )
    }
  }

  test(
    "GET /fulfilments on an empty store returns 200 with [] and X-Total-Count: 0"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments")
      )
      body <- response.as[List[FulfilmentResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body, Nil)
      assertEquals(
        response.headers.get(ci"X-Total-Count").map(_.head.value),
        Some("0")
      )
    }
  }

  test(
    "GET /fulfilments with an offset past the end returns [] and the full total"
  ) {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      _ <- store.create(/* codegen:fields:TEST_CREATE_ARGS */)
      _ <- store.create(/* codegen:fields:TEST_CREATE_ARGS */)
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments".withQueryParam("offset", 5))
      )
      body <- response.as[List[FulfilmentResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body, Nil)
      assertEquals(
        response.headers.get(ci"X-Total-Count").map(_.head.value),
        Some("2")
      )
    }
  }

  test("GET /fulfilments with limit<=0 returns 400") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments".withQueryParam("limit", 0))
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test("GET /fulfilments with limit>100 returns 400") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments".withQueryParam("limit", 101))
      )
    } yield assertEquals(response.status, Status.BadRequest)
  }

  test("GET /fulfilments with offset<0 returns 400") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      routes = FulfilmentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/fulfilments".withQueryParam("offset", -1))
      )
    } yield assertEquals(response.status, Status.BadRequest)
  }
}
