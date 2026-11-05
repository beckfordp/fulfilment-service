package fulfilmentservice

import cats.effect.{IO, IOApp, Resource}
import com.comcast.ip4s._
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder

/** Acquires every dependency this service needs, delegates route assembly to
  * `AppRoutes.all`, and serves the result — a single flat `Resource`
  * for-comprehension, one `<-` per dependency. Every dependency that isn't
  * already a `Resource` is acquired via a `.resource`/`.loadValidated` method
  * on its own owning type (FulfilmentServiceConfig, Migrations, Telemetry), so
  * none of their internals (PureConfig, Flyway, OpenTelemetry) leak into this
  * file. The two exceptions: `EmberServerBuilder` is already
  * `Resource`-returning (http4s' own type, no wrapper needed), and the startup
  * log line stays an explicit `Resource.eval` here since logging isn't itself a
  * dependency. Route construction (`Docs.routes` OpenAPI/Swagger,
  * `AsyncApiDocs.routes` raw AsyncAPI spec, `fileService` static AsyncAPI HTML,
  * the ordering those require, and the tracing/metrics/CORS middleware chain)
  * lives in `AppRoutes`, not here. See `docs/writing-main.md` for the full
  * rationale and order-service for a fuller worked example once a service grows
  * optional dependencies.
  */
object Main extends IOApp.Simple {

  val run: IO[Unit] = (for {
    (config, port) <- Resource.eval(
      FulfilmentServiceConfig.loadValidated[IO]
    )
    _ <- Migrations.resource[IO](config.postgres)
    telemetry <- Telemetry.resource[IO](
      config.serviceName,
      config.metricsPort,
      Some(runtime.metrics)
    )
    _ <- Resource.eval(
      telemetry.logger.info(
        Map(
          "port" -> config.port.toString,
          "metrics_port" -> config.metricsPort.toString
        )
      )("fulfilment-service starting")
    )
    _ <- Resource.eval(
      if (config.operatorAuth.token.exists(_.trim.nonEmpty))
        telemetry.logger.warn(
          Map("operator_auth" -> "enabled")
        )(
          "Dev operator token enabled - reconciliation resolve is local/demo only (ADR 0026)"
        )
      else IO.unit
    )
    store <- FulfilmentStore
      .postgres[IO](config.postgres, telemetry.meter, telemetry.tracer)
      .evalMap(
        FulfilmentJourneyMetrics.instrument[IO](
          _,
          telemetry.meter,
          telemetry.logger
        )
      )
    backlog <- BacklogGauges.postgresQuery[IO](config.postgres)
    _ <- BacklogGauges.register[IO](telemetry.meter)(backlog)
    httpClient <- EmberClientBuilder.default[IO].build
    orders = OrderClient.resilient[IO](
      httpClient,
      config.orderClient,
      telemetry
    )
    _ <- KafkaConsumers.resource[IO](
      config.kafka,
      store,
      orders,
      telemetry.logger,
      telemetry.meter
    )
    routes = AppRoutes.all[IO](
      store,
      telemetry,
      config.simulation.enabled,
      AsyncApiChannels.channels,
      config.asyncApiHtmlDir,
      OperatorAuth.fromConfig(config.operatorAuth)
    )
    _ <- EmberServerBuilder
      .default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port)
      .withHttpApp(routes.orNotFound)
      .build
  } yield ()).useForever
}
