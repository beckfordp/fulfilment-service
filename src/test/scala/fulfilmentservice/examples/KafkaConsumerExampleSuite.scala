package fulfilmentservice.examples

import cats.effect.{IO, Ref}
import fs2.Stream
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.{DeadLetter, StreamSupervisor, StreamSupervisorConfig}
import purekafka.resilience.{
  CircuitBreakerConfig,
  ResilienceConfig,
  RetryConfig
}

import scala.concurrent.duration._

/** This template's reference service doesn't consume from Kafka, so
  * `purekafka`'s consumer-side combinators aren't wired into `Main`. When you
  * adapt this template to consume a Kafka topic, wrap the raw
  * `consumer.stream.evalMap(processRecord)` pipeline in `StreamSupervisor`
  * (restart-with-backoff if the whole stream dies, e.g. a broker disconnect)
  * and guard each message's processing with `DeadLetter` (bounded retry, then
  * route to `<topic>-dlq` instead of killing the stream for one poison message) -
  * as shown here, against synthetic messages rather than a real Kafka broker.
  * See `docs/examples.md`, and order-service's `KafkaConsumers.scala` for the
  * fuller, real-topic version of this same composition.
  */
class KafkaConsumerExampleSuite extends CatsEffectSuite {

  private val supervisorConfig =
    StreamSupervisorConfig(baseDelay = 1.millisecond, maxDelay = 2.milliseconds)

  private val dlqRetryConfig =
    RetryConfig(maxRetries = 2, baseDelay = 1.millisecond)

  private val dlqPublishResilience = ResilienceConfig(
    retry = RetryConfig(maxRetries = 2, baseDelay = 1.millisecond),
    circuitBreaker =
      CircuitBreakerConfig(failureThreshold = 100, resetTimeout = 1.second)
  )

  private def dummyDeadLetter(
      published: Ref[IO, List[(String, String, String)]]
  ): IO[DeadLetter[IO, String, String]] =
    DeadLetter.fromPublish[IO, String, String](
      dlqRetryConfig,
      dlqPublishResilience,
      (topic, key, value) => published.update(_ :+ (topic, key, value))
    )(NoOpLogger[IO])(Meter.noop[IO])

  test(
    "a permanently-unprocessable message is routed to the DLQ, and the stream keeps consuming later messages"
  ) {
    for {
      published <- Ref.of[IO, List[(String, String, String)]](Nil)
      deadLetter <- dummyDeadLetter(published)
      processed <- Ref.of[IO, List[String]](Nil)
      messages = List("ok-1", "poison", "ok-2")
      consumeOnce: IO[Stream[IO, Unit]] = IO.pure(
        Stream.emits(messages).evalMap { value =>
          deadLetter.guard("example-topic", "key", value) {
            if (value == "poison")
              IO.raiseError(new RuntimeException("unprocessable"))
            else processed.update(_ :+ value)
          }
        }
      )
      _ <- StreamSupervisor
        .supervise[IO, Unit](supervisorConfig)(NoOpLogger[IO])(Meter.noop[IO])(
          consumeOnce
        )
        .compile
        .drain
      finalProcessed <- processed.get
      finalPublished <- published.get
    } yield {
      assertEquals(finalProcessed, List("ok-1", "ok-2"))
      assertEquals(
        finalPublished,
        List(("example-topic-dlq", "key", "poison"))
      )
    }
  }

  test(
    "a crash in the stream itself (not just one message) is restarted with backoff"
  ) {
    for {
      published <- Ref.of[IO, List[(String, String, String)]](Nil)
      deadLetter <- dummyDeadLetter(published)
      attempts <- Ref.of[IO, Int](0)
      buildStream: IO[Stream[IO, Unit]] = attempts.updateAndGet(_ + 1).map {
        attempt =>
          if (attempt < 3)
            Stream.raiseError[IO](
              new RuntimeException(s"simulated broker blip, attempt $attempt")
            )
          else
            Stream.eval(deadLetter.guard("example-topic", "key", "ok")(IO.unit))
      }
      _ <- StreamSupervisor
        .supervise[IO, Unit](supervisorConfig)(NoOpLogger[IO])(Meter.noop[IO])(
          buildStream
        )
        .compile
        .drain
      finalAttempts <- attempts.get
    } yield assertEquals(finalAttempts, 3) // 1 initial + 2 restarts
  }
}
