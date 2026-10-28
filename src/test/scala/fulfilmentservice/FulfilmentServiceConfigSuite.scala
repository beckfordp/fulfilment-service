package fulfilmentservice

import cats.effect.IO
import munit.CatsEffectSuite
import pureconfig.ConfigSource
import purekafka.consumer.StreamSupervisorConfig
import purekafka.resilience.{
  CircuitBreakerConfig => KafkaCircuitBreakerConfig,
  ResilienceConfig => KafkaResilienceConfig,
  RetryConfig => KafkaRetryConfig
}
import purerest.resilience.{CircuitBreakerConfig, ResilienceConfig, RetryConfig}

import scala.concurrent.duration._

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
      |order-client {
      |  base-url = "http://localhost:8083"
      |  resilience {
      |    retry { max-retries = 3, base-delay = 100ms }
      |    circuit-breaker { failure-threshold = 5, reset-timeout = 30s }
      |  }
      |}
      |simulation { enabled = false }
      |kafka {
      |  bootstrap-servers = "localhost:9092"
      |  dlq-retry { max-retries = 3, base-delay = 200ms }
      |  dlq-publish-resilience {
      |    retry { max-retries = 3, base-delay = 100ms }
      |    circuit-breaker { failure-threshold = 5, reset-timeout = 30s }
      |  }
      |  stream-supervisor { base-delay = 1s, max-delay = 30s }
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
          ),
          orderClient = OrderClientConfig(
            baseUrl = "http://localhost:8083",
            resilience = ResilienceConfig(
              retry = RetryConfig(maxRetries = 3, baseDelay = 100.millis),
              circuitBreaker = CircuitBreakerConfig(
                failureThreshold = 5,
                resetTimeout = 30.seconds
              )
            )
          ),
          simulation = SimulationConfig(enabled = false),
          kafka = KafkaConfig(
            bootstrapServers = "localhost:9092",
            dlqRetry = KafkaRetryConfig(3, 200.millis),
            dlqPublishResilience = KafkaResilienceConfig(
              retry = KafkaRetryConfig(3, 100.millis),
              circuitBreaker = KafkaCircuitBreakerConfig(5, 30.seconds)
            ),
            streamSupervisor = StreamSupervisorConfig(1.second, 30.seconds)
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
      assertEquals(config.orderClient.baseUrl, "http://localhost:8083")
      assertEquals(config.simulation, SimulationConfig(enabled = false))
      assertEquals(config.kafka.bootstrapServers, "localhost:9092")
    }
  }
}
