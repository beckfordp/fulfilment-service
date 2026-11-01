package fulfilmentservice

import munit.FunSuite
import purekafka.docs.{AsyncApiDoc, ChannelDirection, PublisherKind}

class AsyncApiChannelsSuite extends FunSuite {

  test("registers the consumed and produced fulfilment contracts") {
    val channels = AsyncApiChannels.channels
      .map(c => (c.topic, c.eventType, c.direction))
      .toSet

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

  test(
    "every consumed event's generated AsyncAPI message carries a description (copied from the producer's registry artifact)"
  ) {
    val doc = AsyncApiDoc.generate(
      "test",
      "1.0",
      AsyncApiChannels.channels
    )
    val consumed = AsyncApiChannels.channels.filter(
      _.direction == ChannelDirection.Subscribe
    )
    assert(consumed.nonEmpty)
    for (c <- consumed)
      assert(
        doc.hcursor
          .downField("channels")
          .downField(c.topic)
          .downField("messages")
          .downField(c.eventType)
          .downField("payload")
          .downField("description")
          .as[String]
          .isRight,
        s"${c.topic}/${c.eventType} has no description"
      )
  }
}
