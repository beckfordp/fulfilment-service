package fulfilmentservice

import fulfilmentservice.domain.{FulfilmentState}

import fulfilmentservice.domain.OrderId
import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.effect.testkit.TestControl
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

class FulfilmentDomainSuite extends CatsEffectSuite {

  private val orderId = OrderId.fromUuid(UUID.randomUUID())
  private val rendered = OrderId.render(orderId)

  private val item = FulfilmentItem("sku-1", "Watch", 2, 1000)

  test("parses every state from its wire value") {
    assertEquals(
      List("PENDING", "PICKING", "PACKED", "DISPATCHED", "DISPATCH_FAILED")
        .flatMap(FulfilmentState.fromString),
      FulfilmentState.values.toList
    )
    assertEquals(FulfilmentState.fromString("pending"), None)
  }

  test("DispatchConfirmed envelope carries the full item snapshot") {
    FulfilmentEvents
      .dispatchConfirmed[cats.effect.IO](orderId, "GLN-1", List(item))
      .map { envelope =>
        assertEquals(envelope.eventType, "DispatchConfirmed")
        assertEquals(envelope.source, "fulfilment-service")
        assertEquals(envelope.correlationId, rendered)
        val payload = envelope.payload
          .as[DispatchConfirmedEvent]
          .toOption
          .get
        assertEquals(payload.orderId, rendered)
        assertEquals(payload.trackingNo, "GLN-1")
        assertEquals(payload.items, List(item))
      }
  }

  test("DispatchFailed envelope carries the reason") {
    FulfilmentEvents
      .dispatchFailed[cats.effect.IO](orderId, "damaged")
      .map { envelope =>
        assertEquals(envelope.eventType, "DispatchFailed")
        assertEquals(
          envelope.payload.as[DispatchFailedEvent].toOption,
          Some(DispatchFailedEvent(rendered, "damaged"))
        )
      }
  }

  test("envelope id and timestamp come from UUIDGen[F] and Clock[F]") {
    val fixed = UUID.fromString("00000000-0000-4000-8000-000000000001")
    given UUIDGen[IO] = new UUIDGen[IO] {
      def randomUUID: IO[UUID] = IO.pure(fixed)
    }
    TestControl.executeEmbed(
      FulfilmentEvents.dispatchFailed[IO](orderId, "damaged").map { envelope =>
        assertEquals(envelope.eventId, fixed.toString)
        assertEquals(envelope.timestamp, Instant.EPOCH)
      }
    )
  }
}
