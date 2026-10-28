package fulfilmentservice

import cats.effect.Async
import cats.syntax.semigroupk._
import fs2.io.file.Files
import org.http4s.HttpRoutes
import org.http4s.server.staticcontent.{FileService, fileService}
import purekafka.docs.{AsyncApiChannel, Docs => AsyncApiDocs}
import purerest.docs.Docs
import purerest.metrics.ServerMetrics
import purerest.tracing.ServerTracing

/** Assembles this service's full set of `HttpRoutes[F]` — business routes,
  * OpenAPI/Swagger docs, AsyncAPI docs (raw spec + rendered static HTML) — and
  * applies the full middleware stack (server tracing, server metrics, CORS), in
  * one call. Extracted out of `Main` so `Main` only acquires dependencies and
  * calls this once, per the `Resource`-built-module composition pattern
  * documented in purerest's
  * `docs/functional-design-patterns-scala-haskell-category-theory.md`.
  *
  * `staticAsyncApiDocsDir` is defaulted to production's path; `AppRoutesSuite`
  * overrides it with a temporary fixture directory so tests don't depend on
  * build-time generated HTML (which requires a Node toolchain).
  */
object AppRoutes {

  def all[F[_]: Async](
      store: FulfilmentStore[F],
      telemetry: Telemetry[F],
      channels: List[AsyncApiChannel[?]],
      staticAsyncApiDocsDir: String =
        sys.env.getOrElse("ASYNCAPI_HTML_DIR", "target/asyncapi-docs")
  ): HttpRoutes[F] = {
    // Explicit, non-deprecated Files[F] - without it, fileService[F] below
    // falls back to fs2's deprecated Files.implicitForAsync conversion.
    implicit val filesForF: Files[F] = Files.forAsync[F]

    val docsRoutes = Docs.routes[F](
      "Fulfilment Service",
      "1.0",
      List(
        FulfilmentRoutes.serverEndpoint[F](store, telemetry.logger),
        FulfilmentRoutes.getFulfilmentServerEndpoint[F](
          store,
          telemetry.logger
        ),
        FulfilmentRoutes.updateFulfilmentServerEndpoint[F](
          store,
          telemetry.logger
        ),
        FulfilmentRoutes.replaceFulfilmentServerEndpoint[F](
          store,
          telemetry.logger
        ),
        FulfilmentRoutes.deleteFulfilmentServerEndpoint[F](
          store,
          telemetry.logger
        ),
        FulfilmentRoutes.listFulfilmentsServerEndpoint[F](
          store,
          telemetry.logger
        ),
        HealthRoutes.healthServerEndpoint[F],
        HealthRoutes.readyServerEndpoint[F](store)
      )
    )

    val asyncApiRoutes = AsyncApiDocs.routes[F](
      "Fulfilment Service",
      "1.0",
      channels
    )

    // Serves the plugin's target/asyncapi-docs/index.html as static HTML at
    // /docs/asyncapi/* - sibling to asyncApiRoutes' raw-spec route, same split
    // as Swagger's /docs vs /docs/docs.yaml. Docker builds place the generated
    // files at this same relative path under /opt/docker (see build.sbt).
    val staticAsyncApiRoutes = fileService[F](
      FileService.Config(
        systemPath = staticAsyncApiDocsDir,
        pathPrefix = "/docs/asyncapi"
      )
    )

    // asyncApiRoutes/staticAsyncApiRoutes must come before docsRoutes:
    // docsRoutes' Swagger UI bundle serves a wildcard /docs/{resource} route
    // for its own static assets, which returns its own real 404 (not an http4s
    // fallthrough None) for any /docs/* path it doesn't recognize - including
    // /docs/asyncapi.json - short-circuiting <+> before asyncApiRoutes ever
    // sees the request.
    val tracedRoutes = ServerTracing.middleware(telemetry.tracer)(
      asyncApiRoutes <+> staticAsyncApiRoutes <+> docsRoutes
    )
    val meteredRoutes =
      ServerMetrics.middleware[F](telemetry.meter)(tracedRoutes)
    Cors.middleware[F](meteredRoutes)
  }
}
