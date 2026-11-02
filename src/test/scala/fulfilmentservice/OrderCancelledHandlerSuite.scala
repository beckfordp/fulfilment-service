package fulfilmentservice

import cats.effect.{IO, Ref}
import cats.syntax.all._
import fulfilmentservice.StoreCommands._
import fulfilmentservice.domain.{FulfilmentState, OrderId}
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.DeadLetter
import purekafka.resilience.{
  CircuitBreakerConfig,
  ResilienceConfig,
  RetryConfig
}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

/** `OrderCancelledConsumer.handle` without a broker (US-9.5). */
class OrderCancelledHandlerSuite extends CatsEffectSuite {

  private type Dlq = Ref[IO, List[(String, String, String)]]

  private def deadLetter(dlq: Dlq): IO[DeadLetter[IO, String, String]] = {
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    DeadLetter.fromPublish[IO, String, String](
      RetryConfig(maxRetries = 0, baseDelay = 1.milli),
      ResilienceConfig(
        retry = RetryConfig(maxRetries = 0, baseDelay = 1.milli),
        circuitBreaker =
          CircuitBreakerConfig(failureThreshold = 100, resetTimeout = 1.second)
      ),
      (topic, key, value) => dlq.update(_ :+ (topic, key, value))
    )(NoOpLogger[IO])(summon[Meter[IO]])
  }

  private val item = FulfilmentItem("SKU-0042", "Espresso cup", 1, 1000)
  private val address = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private def envelope(orderId: OrderId): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString,
      eventType = "OrderCancelled",
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "order-service",
      correlationId = OrderId.render(orderId),
      payload = OrderCancelledEvent(
        OrderId.render(orderId),
        "cust-1",
        Instant.parse("2026-01-01T00:00:00Z"),
        Some("customer")
      ).asJson
    ).asJson.noSpaces

  private def stateAfter(
      setup: FulfilmentStore[IO] => IO[Unit],
      orderId: OrderId
  ) =
    for {
      store <- FulfilmentStore.inMemory[IO]
      _ <- store.createTask(orderId, List(item), address)
      _ <- setup(store)
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      _ <- OrderCancelledConsumer.handle(store, NoOpLogger[IO], dl)(
        OrderId.render(orderId),
        Some(envelope(orderId))
      )
      task <- store.get(orderId)
      routed <- dlq.get
    } yield (task.map(_.state), routed)

  private def id =
    OrderId.fromUuid(UUID.fromString("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f"))

  test("OrderCancelled aborts a PENDING task") {
    stateAfter(_ => IO.unit, id).map { case (state, routed) =>
      assertEquals(state, Some(FulfilmentState.Cancelled))
      assertEquals(routed, Nil)
    }
  }

  test("OrderCancelled aborts a PICKING task") {
    stateAfter(_.startPicking(id).void, id).map { case (state, _) =>
      assertEquals(state, Some(FulfilmentState.Cancelled))
    }
  }

  test("OrderCancelled aborts a PACKED task") {
    stateAfter(store => store.startPicking(id).void *> store.pack(id).void, id)
      .map { case (state, _) =>
        assertEquals(state, Some(FulfilmentState.Cancelled))
      }
  }

  test("OrderCancelled leaves a DISPATCHED task final") {
    stateAfter(
      store =>
        store.startPicking(id).void *> store.pack(id).void *>
          store.dispatch(id, "GLN-1", "DHL").void,
      id
    ).map { case (state, _) =>
      assertEquals(state, Some(FulfilmentState.Dispatched))
    }
  }

  test("a duplicate OrderCancelled is an idempotent no-op") {
    val orderId = id
    (for {
      store <- FulfilmentStore.inMemory[IO]
      _ <- store.createTask(orderId, List(item), address)
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      handle = OrderCancelledConsumer.handle(store, NoOpLogger[IO], dl) _
      _ <- handle(OrderId.render(orderId), Some(envelope(orderId)))
      _ <- handle(OrderId.render(orderId), Some(envelope(orderId)))
      task <- store.get(orderId)
      routed <- dlq.get
    } yield {
      assertEquals(task.map(_.state), Some(FulfilmentState.Cancelled))
      assertEquals(routed, Nil)
    })
  }

  test("OrderCancelled for an unknown task is skipped") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      _ <- OrderCancelledConsumer.handle(store, NoOpLogger[IO], dl)(
        OrderId.render(id),
        Some(envelope(id))
      )
      routed <- dlq.get
    } yield assertEquals(routed, Nil)
  }

  test("a malformed OrderCancelled payload is dead-lettered") {
    for {
      store <- FulfilmentStore.inMemory[IO]
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      _ <- OrderCancelledConsumer.handle(store, NoOpLogger[IO], dl)(
        "key",
        Some("""{"not":"an envelope"}""")
      )
      routed <- dlq.get
    } yield assertEquals(routed.map(_._1), List("order-events-dlq"))
  }
}
