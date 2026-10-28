package fulfilmentservice

import purekafka.docs.{
  AsyncApiChannel,
  AsyncApiChannelsProvider,
  ChannelDirection,
  PublisherKind
}

/** Kafka contracts for AsyncAPI docs (ADR 0010): consumes `PaymentAuthorized`
  * on `payment-events`; publishes `DispatchConfirmed`/`DispatchFailed` on
  * `fulfilment-events` via the outbox + Debezium (`Publish(Cdc)`).
  */
object AsyncApiChannels extends AsyncApiChannelsProvider {
  val channels: List[AsyncApiChannel[?]] = List(
    AsyncApiChannel[PaymentAuthorizedEvent](
      topic = "payment-events",
      eventType = "PaymentAuthorized",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[DispatchConfirmedEvent](
      topic = "fulfilment-events",
      eventType = "DispatchConfirmed",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    ),
    AsyncApiChannel[DispatchFailedEvent](
      topic = "fulfilment-events",
      eventType = "DispatchFailed",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    )
  )
}
