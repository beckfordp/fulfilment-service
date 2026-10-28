package fulfilmentservice

import cats.effect.{IO, Resource}
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{HttpRoutes, Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import purerest.metrics.Metrics
import purerest.tracing.Tracing

import fulfilmentservice.examples.ExampleAsyncApiChannels

import scala.jdk.CollectionConverters._

class AppRoutesSuite extends CatsEffectSuite {

  private def withFixtureDir[A](f: java.nio.file.Path => IO[A]): IO[A] =
    Resource
      .make(
        IO.blocking {
          val dir =
            java.nio.file.Files.createTempDirectory("app-routes-asyncapi")
          java.nio.file.Files.writeString(
            dir.resolve("index.html"),
            "<html><body>asyncapi fixture</body></html>"
          )
          dir
        }
      )(dir =>
        IO.blocking {
          val paths = java.nio.file.Files.walk(dir)
          try
            paths.iterator().asScala.toList.reverse.foreach { path =>
              java.nio.file.Files.deleteIfExists(path)
              ()
            }
          finally paths.close()
        }.void
      )
      .use(f)

  private def withRoutes[A](f: HttpRoutes[IO] => IO[A]): IO[A] =
    withFixtureDir { fixtureDir =>
      (for {
        testTracer <- Tracing.test[IO]("app-routes-suite")
        testMeter <- Metrics.test[IO]("app-routes-suite")
        store <- Resource.eval(FulfilmentStore.inMemory[IO])
      } yield AppRoutes.all[IO](
        store,
        Telemetry(testTracer.tracer, testMeter.meter, NoOpLogger[IO]),
        ExampleAsyncApiChannels.channels,
        staticAsyncApiDocsDir = fixtureDir.toString
      )).use(f)
    }

  test("GET /docs (Swagger UI) resolves, not a 404") {
    withRoutes { routes =>
      routes.orNotFound
        .run(Request[IO](Method.GET, uri"/docs"))
        .map(response => assertNotEquals(response.status, Status.NotFound))
    }
  }

  test(
    "GET /docs/asyncapi.json resolves and contains the registered event type"
  ) {
    withRoutes { routes =>
      for {
        response <- routes.orNotFound.run(
          Request[IO](Method.GET, uri"/docs/asyncapi.json")
        )
        body <- response.bodyText.compile.string
      } yield {
        assertEquals(response.status, Status.Ok)
        assert(
          body.contains("ExampleEvent"),
          s"expected the registered channel's eventType in the AsyncAPI doc, got: $body"
        )
      }
    }
  }

  test(
    "GET /docs/asyncapi/index.html serves the configured static HTML directory"
  ) {
    withRoutes { routes =>
      for {
        response <- routes.orNotFound.run(
          Request[IO](Method.GET, uri"/docs/asyncapi/index.html")
        )
        body <- response.bodyText.compile.string
      } yield {
        assertEquals(response.status, Status.Ok)
        assert(body.contains("asyncapi fixture"))
      }
    }
  }

  test("POST /fulfilments resolves and creates an entity end-to-end") {
    val createRequest =
      CreateFulfilmentRequest(/* codegen:fields:TEST_CREATE_REQUEST_ARGS */)
    withRoutes { routes =>
      routes.orNotFound
        .run(
          Request[IO](Method.POST, uri"/fulfilments")
            .withEntity(createRequest)
        )
        .map(response => assertEquals(response.status, Status.Created))
    }
  }
}
