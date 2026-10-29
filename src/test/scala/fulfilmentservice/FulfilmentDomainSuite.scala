package fulfilmentservice

import fulfilmentservice.domain.OrderId
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

  test("marks only DISPATCHED/DISPATCH_FAILED terminal") {
    assert(!FulfilmentState.isTerminal(FulfilmentState.Pending))
    assert(!FulfilmentState.isTerminal(FulfilmentState.Picking))
    assert(!FulfilmentState.isTerminal(FulfilmentState.Packed))
    assert(FulfilmentState.isTerminal(FulfilmentState.Dispatched))
    assert(FulfilmentState.isTerminal(FulfilmentState.DispatchFailed))
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
}
