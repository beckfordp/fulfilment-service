package fulfilmentservice

import cats.effect.IO
import cats.syntax.all._
import fulfilmentservice.domain.{FulfilmentState, OrderId}
import munit.CatsEffectSuite
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.{
  ERROR,
  INFO,
  WARN
}

import java.time.Instant
import java.util.UUID

/** Broker-free tests of `PaymentAuthorizedConsumer.handle` (TD-16.3 M4). */
class PaymentAuthorizedHandlerSuite extends CatsEffectSuite {

  private val orderId = OrderId.fromUuid(
    UUID.fromString("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
  )
  private val item = FulfilmentItem("SKU-0042", "Espresso cup", 2, 1250)
  private val address = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private def event(rawOrderId: String): PaymentAuthorizedEvent =
    PaymentAuthorizedEvent(
      rawOrderId,
      "auth-7f3a9c",
      Instant.parse("2026-11-07T10:15:30Z"),
      Instant.parse("2026-10-31T10:15:30Z")
    )

  private def orders(lookup: OrderLookup): OrderClient[IO] =
    new OrderClient[IO] {
      def get(id: OrderId): IO[OrderLookup] = IO.pure(lookup)
    }

  private def run(lookup: OrderLookup, events: PaymentAuthorizedEvent*) =
    for {
      store <- FulfilmentStore.inMemory[IO]
      logger = StructuredTestingLogger.impl[IO]()
      _ <- events.toList.traverse_(
        PaymentAuthorizedConsumer.handle[IO](store, orders(lookup), logger)
      )
      tasks <- store.list(None)
      logged <- logger.logged
    } yield (tasks, logged)

  test("creates a PENDING task from the order snapshot") {
    run(
      OrderLookup.Found(List(item), address),
      event(OrderId.render(orderId))
    ).map { case (tasks, _) =>
      assertEquals(tasks.map(_.orderId), List(orderId))
      assertEquals(tasks.map(_.state), List(FulfilmentState.Pending))
      assertEquals(tasks.flatMap(_.items), List(item))
    }
  }

  test("a redelivered event leaves one unchanged task") {
    run(
      OrderLookup.Found(List(item), address),
      event(OrderId.render(orderId)),
      event(OrderId.render(orderId))
    ).map { case (tasks, _) => assertEquals(tasks.size, 1) }
  }

  test("an unknown order is skipped with a warning") {
    run(OrderLookup.NotFound, event(OrderId.render(orderId))).map {
      case (tasks, logged) =>
        assertEquals(tasks, Nil)
        assert(logged.exists {
          case WARN(message, _, _) => message.contains("unknown order")
          case _                   => false
        })
    }
  }

  test("an order without a shipping address is skipped with its own error") {
    run(OrderLookup.NoShippingAddress, event(OrderId.render(orderId))).map {
      case (tasks, logged) =>
        assertEquals(tasks, Nil)
        assert(logged.exists {
          case ERROR(message, _, _) => message.contains("no shipping address")
          case _                    => false
        })
    }
  }

  test("an invalid orderId is logged and dropped") {
    run(OrderLookup.Found(List(item), address), event("not-a-uuid")).map {
      case (tasks, logged) =>
        assertEquals(tasks, Nil)
        assert(logged.exists {
          case ERROR(message, _, _) => message.contains("Invalid orderId")
          case _                    => false
        })
        assert(!logged.exists {
          case INFO(_, _, _) => true
          case _             => false
        })
    }
  }
}
