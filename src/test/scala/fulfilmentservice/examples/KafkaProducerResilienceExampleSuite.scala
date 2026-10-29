package fulfilmentservice.examples

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import pureconfig.{ConfigReader, ConfigSource}
import purekafka.resilience.{
  CircuitBreakerConfig,
  CircuitBreakerOpen,
  Resilience,
  ResilienceConfig,
  RetryConfig
}

/** This template's reference service doesn't publish to Kafka directly, so
  * `purekafka`'s retry + circuit-breaker middleware isn't wired into `Main`.
  * When you adapt this template to call a Kafka producer directly (not via the
  * outbox-CDC pattern — see `OutboxExampleSuite`), wrap the publish call with
  * `Resilience.middleware` before sending, as shown here — this test exercises
  * that exact pattern against a dummy publish function, so it stays correct as
  * `purekafka` evolves. See `docs/examples.md`.
  */
class KafkaProducerResilienceExampleSuite extends CatsEffectSuite {

  // purekafka's resilience case classes don't derive ConfigReader themselves
  // (the library has no PureConfig dependency) - instances are derived here
  // instead, the same way a real service's config module would.
  private given ConfigReader[RetryConfig] = ConfigReader.derived
  private given ConfigReader[CircuitBreakerConfig] = ConfigReader.derived
  private given ConfigReader[ResilienceConfig] = ConfigReader.derived

  // Loaded from application.conf's "example-kafka-producer" block - not read
  // by the live service, only by this example.
  private val exampleKafkaProducerConfig: ResilienceConfig =
    ConfigSource.default
      .at("example-kafka-producer")
      .loadOrThrow[ResilienceConfig]

  private def dummyPublish(
      counter: Ref[IO, Int]
  )(behavior: Int => Either[Throwable, Unit]): (String, String) => IO[Unit] =
    (_, _) =>
      counter.updateAndGet(_ + 1).flatMap(n => IO.fromEither(behavior(n)))

  test("a transient publish failure is retried until it succeeds") {
    for {
      counter <- Ref.of[IO, Int](0)
      publish = dummyPublish(counter)(n =>
        if (n < 3) Left(new RuntimeException(s"boom $n")) else Right(())
      )
      resilientPublish = Resilience.middleware[IO](
        exampleKafkaProducerConfig
      )(NoOpLogger[IO])(Meter.noop[IO])
      _ <- resilientPublish(publish("key", "value"))
      attempts <- counter.get
    } yield assertEquals(attempts, 3)
  }

  test(
    "the circuit breaker opens after repeated publish failures and fails fast"
  ) {
    for {
      counter <- Ref.of[IO, Int](0)
      publish = dummyPublish(counter)(_ =>
        Left(new RuntimeException("always fails"))
      )
      // Same PureConfig-loaded config, adjusted to isolate the circuit
      // breaker in this test: no retries, and a lower failure threshold so
      // it trips after just two calls instead of
      // exampleKafkaProducerConfig's 10.
      config = exampleKafkaProducerConfig.copy(
        retry = exampleKafkaProducerConfig.retry.copy(maxRetries = 0),
        circuitBreaker =
          exampleKafkaProducerConfig.circuitBreaker.copy(failureThreshold = 2)
      )
      resilientPublish =
        Resilience.middleware[IO](config)(NoOpLogger[IO])(Meter.noop[IO])
      _ <- resilientPublish(publish("key", "value")).attempt
      _ <- resilientPublish(publish("key", "value")).attempt
      openResult <- resilientPublish(publish("key", "value")).attempt
      attempts <- counter.get
    } yield {
      assertEquals(openResult, Left(CircuitBreakerOpen))
      // the third call was rejected by the open breaker before ever reaching
      // the dummy publish function
      assertEquals(attempts, 2)
    }
  }
}
