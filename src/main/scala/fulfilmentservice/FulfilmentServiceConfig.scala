package fulfilmentservice

import cats.effect.Sync
import cats.syntax.all._
import com.comcast.ip4s.Port
import pureconfig.{ConfigReader, ConfigSource}
import purekafka.consumer.StreamSupervisorConfig
import purekafka.resilience.{
  CircuitBreakerConfig => KafkaCircuitBreakerConfig,
  ResilienceConfig => KafkaResilienceConfig,
  RetryConfig => KafkaRetryConfig
}
import purerest.resilience.ResilienceConfig

final case class PostgresConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
) derives ConfigReader

final case class OrderClientConfig(
    baseUrl: String,
    resilience: ResilienceConfig
) derives ConfigReader

final case class SimulationConfig(enabled: Boolean) derives ConfigReader

given ConfigReader[KafkaCircuitBreakerConfig] = ConfigReader.derived
given ConfigReader[KafkaResilienceConfig] = ConfigReader.derived
given ConfigReader[StreamSupervisorConfig] = ConfigReader.derived

final case class KafkaConfig(
    bootstrapServers: String,
    dlqRetry: KafkaRetryConfig,
    dlqPublishResilience: KafkaResilienceConfig,
    streamSupervisor: StreamSupervisorConfig
) derives ConfigReader

final case class FulfilmentServiceConfig(
    port: Int,
    metricsPort: Int,
    serviceName: String,
    postgres: PostgresConfig,
    orderClient: OrderClientConfig,
    simulation: SimulationConfig,
    kafka: KafkaConfig
) derives ConfigReader

object FulfilmentServiceConfig {
  def load[F[_]: Sync]: F[FulfilmentServiceConfig] =
    Sync[F].delay(ConfigSource.default.loadOrThrow[FulfilmentServiceConfig])

  /** `load`, plus parsing/validating `port` as an `ip4s.Port` - so `Main`
    * doesn't have to do that validation itself.
    */
  def loadValidated[F[_]: Sync]: F[(FulfilmentServiceConfig, Port)] =
    for {
      config <- load[F]
      port <- Sync[F].fromOption(
        Port.fromInt(config.port),
        new IllegalArgumentException(
          s"Invalid fulfilment-service port: ${config.port}"
        )
      )
    } yield (config, port)
}
