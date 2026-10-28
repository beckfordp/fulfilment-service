package fulfilmentservice

import munit.FunSuite
import purekafka.docs.{ChannelDirection, PublisherKind}

class AsyncApiChannelsSuite extends FunSuite {

  test("registers the consumed and produced fulfilment contracts") {
    val channels = AsyncApiChannels.channels.map(c =>
      (c.topic, c.eventType, c.direction)
    ).toSet

    assertEquals(
      channels,
      Set(
        (
          "payment-events",
          "PaymentAuthorized",
          ChannelDirection.Subscribe: ChannelDirection
        ),
        (
          "fulfilment-events",
          "DispatchConfirmed",
          ChannelDirection.Publish(PublisherKind.Cdc): ChannelDirection
        ),
        (
          "fulfilment-events",
          "DispatchFailed",
          ChannelDirection.Publish(PublisherKind.Cdc): ChannelDirection
        )
      )
    )
  }
}
