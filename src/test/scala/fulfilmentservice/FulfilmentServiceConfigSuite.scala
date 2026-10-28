package fulfilmentservice

import cats.effect.IO
import munit.CatsEffectSuite
import pureconfig.ConfigSource

class FulfilmentServiceConfigSuite extends CatsEffectSuite {

  private val validHocon =
    """
      |port = 8080
      |metrics-port = 9090
      |service-name = "fulfilment-service"
      |postgres {
      |  host = "localhost"
      |  port = 5432
      |  database = "fulfilment"
      |  user = "fulfilment"
      |  password = "fulfilment"
      |}
      |""".stripMargin

  test("loads a fully-specified config") {
    val result =
      ConfigSource.string(validHocon).load[FulfilmentServiceConfig]
    assertEquals(
      result,
      Right(
        FulfilmentServiceConfig(
          port = 8080,
          metricsPort = 9090,
          serviceName = "fulfilment-service",
          postgres = PostgresConfig(
            host = "localhost",
            port = 5432,
            database = "fulfilment",
            user = "fulfilment",
            password = "fulfilment"
          )
        )
      )
    )
  }

  test("fails to load when a required field is missing") {
    val missingPassword =
      """
        |port = 8080
        |metrics-port = 9090
        |postgres {
        |  host = "localhost"
        |  port = 5432
        |  database = "fulfilment"
        |  user = "fulfilment"
        |}
        |""".stripMargin

    assert(
      ConfigSource
        .string(missingPassword)
        .load[FulfilmentServiceConfig]
        .isLeft
    )
  }

  test("load[F] reads the shipped application.conf defaults") {
    FulfilmentServiceConfig.load[IO].map { config =>
      assertEquals(config.port, 8080)
      assertEquals(config.metricsPort, 9090)
      assertEquals(config.serviceName, "fulfilment-service")
      assertEquals(
        config.postgres,
        PostgresConfig(
          "localhost",
          5432,
          "fulfilment",
          "fulfilment",
          "fulfilment"
        )
      )
    }
  }
}
