package fulfilmentservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.{Header, HttpRoutes, Method, Request, Status}
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.typelevel.ci._

class CorsSuite extends CatsEffectSuite {

  private val dummyRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "widgets"  => Ok("[]")
    case POST -> Root / "widgets" => Created("{}")
  }

  private val corsRoutes = Cors.middleware[IO](dummyRoutes)

  test("a cross-origin GET receives Access-Control-Allow-Origin") {
    val request = Request[IO](Method.GET, uri"/widgets")
      .putHeaders(Header.Raw(ci"Origin", "http://example.com"))
    for {
      response <- corsRoutes.orNotFound.run(request)
    } yield assert(response.headers.get(ci"Access-Control-Allow-Origin").isDefined)
  }

  test(
    "an OPTIONS preflight for POST receives the expected allow-methods/headers"
  ) {
    val request = Request[IO](Method.OPTIONS, uri"/widgets")
      .putHeaders(
        Header.Raw(ci"Origin", "http://example.com"),
        Header.Raw(ci"Access-Control-Request-Method", "POST")
      )
    for {
      response <- corsRoutes.orNotFound.run(request)
      allowMethods = response.headers.get(ci"Access-Control-Allow-Methods")
      allowHeaders = response.headers.get(ci"Access-Control-Allow-Headers")
    } yield {
      assert(response.status.isSuccess)
      assert(allowMethods.exists(_.head.value.contains("POST")))
      assert(allowHeaders.exists(_.head.value.toLowerCase.contains("content-type")))
    }
  }

  test("a cross-origin GET exposes X-Total-Count to browser JS") {
    val routesWithCount: HttpRoutes[IO] = HttpRoutes.of[IO] {
      case GET -> Root / "widgets" =>
        Ok("[]").map(_.putHeaders(Header.Raw(ci"X-Total-Count", "0")))
    }
    val request = Request[IO](Method.GET, uri"/widgets")
      .putHeaders(Header.Raw(ci"Origin", "http://example.com"))
    for {
      response <- Cors.middleware[IO](routesWithCount).orNotFound.run(request)
      exposeHeaders = response.headers.get(ci"Access-Control-Expose-Headers")
    } yield {
      assert(
        response.headers.get(ci"X-Total-Count").isDefined,
        "expected the route's own X-Total-Count header to survive the middleware"
      )
      assert(
        exposeHeaders.exists(
          _.head.value.split(",").map(_.trim).contains("X-Total-Count")
        ),
        s"expected Access-Control-Expose-Headers to include X-Total-Count, got: $exposeHeaders"
      )
    }
  }

  test("no Access-Control-Allow-Credentials header is sent") {
    val request = Request[IO](Method.GET, uri"/widgets")
      .putHeaders(Header.Raw(ci"Origin", "http://example.com"))
    for {
      response <- corsRoutes.orNotFound.run(request)
    } yield assertEquals(
      response.headers.get(ci"Access-Control-Allow-Credentials"),
      None
    )
  }
}
