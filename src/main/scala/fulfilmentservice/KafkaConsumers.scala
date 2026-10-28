package fulfilmentservice

import cats.effect.{Async, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.kafka.ProducerSettings
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.{DeadLetter, StreamSupervisor}

/** Wires the single `PaymentAuthorized` consumer with purekafka's supervisor
  * and DLQ (TD-2.3/TD-2.4, ADR 0010). A future consumer gets its own supervisor
  * and is merged in.
  */
object KafkaConsumers {

  def supervised[F[_]: Async](
      config: KafkaConfig,
      store: FulfilmentStore[F],
      orders: OrderClient[F],
      logger: StructuredLogger[F],
      meter: Meter[F]
  ): Stream[F, Unit] = {
    def dlqProducerSettings: ProducerSettings[F, String, String] =
      ProducerSettings[F, String, String]
        .withBootstrapServers(config.bootstrapServers)

    for {
      deadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      result <- StreamSupervisor.supervise[F, Unit](
        config.streamSupervisor
      )(logger)(meter)(
        Async[F].pure(
          PaymentAuthorizedConsumer.run[F](
            config,
            store,
            orders,
            logger,
            deadLetter
          )
        )
      )
    } yield result
  }

  /** Backgrounded as one Resource line in Main. */
  def resource[F[_]: Async](
      config: KafkaConfig,
      store: FulfilmentStore[F],
      orders: OrderClient[F],
      logger: StructuredLogger[F],
      meter: Meter[F]
  ): Resource[F, Unit] =
    supervised[F](
      config,
      store,
      orders,
      logger,
      meter
    ).compile.drain.background.void
}
