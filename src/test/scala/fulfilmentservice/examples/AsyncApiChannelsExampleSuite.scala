package fulfilmentservice.examples

import io.circe.parser.parse
import munit.FunSuite
import purekafka.docs.{
  AsyncApiChannel,
  AsyncApiDoc,
  AsyncApiSpecWriter,
  ChannelDirection
}

import java.nio.file.Files

class AsyncApiChannelsExampleSuite extends FunSuite {

  test(
    "ExampleAsyncApiChannels registers exactly the illustrative ExampleEvent channel"
  ) {
    assertEquals(
      ExampleAsyncApiChannels.channels.map(c => c.topic -> c.eventType),
      List("fulfilment-events" -> "ExampleEvent")
    )
  }

  test("the illustrative channel is Subscribe-only") {
    assertEquals(
      ExampleAsyncApiChannels.channels.map(_.direction),
      List(ChannelDirection.Subscribe: ChannelDirection)
    )
  }

  test(
    "fulfilmentservice.examples.ExampleAsyncApiChannels is reflectively loadable by name, matching build.sbt's asyncApiChannelsClass setting - a regression guard for sbt-purekafka-docs' AsyncApiSpecWriter"
  ) {
    val outputPath =
      Files.createTempFile("fulfilment-service-asyncapi-test", ".json")
    outputPath.toFile.deleteOnExit()

    AsyncApiSpecWriter.main(
      Array(
        "fulfilmentservice.examples.ExampleAsyncApiChannels",
        "Fulfilment Service",
        "1.0",
        outputPath.toString
      )
    )

    val written = parse(Files.readString(outputPath)).toOption.get
    val expected = AsyncApiDoc.generate(
      "Fulfilment Service",
      "1.0",
      ExampleAsyncApiChannels.channels
    )

    assertEquals(written, expected)
  }

  test(
    "the generated AsyncAPI document constrains ExampleEvent.status to the enum [created, updated, deleted]"
  ) {
    val doc = AsyncApiDoc.generate(
      "Fulfilment Service",
      "1.0",
      ExampleAsyncApiChannels.channels
    )

    // String-based, not a guessed cursor path through channels/subscribe/
    // message/oneOf - robust to exactly how AsyncApiDoc nests a
    // single-message channel's schema, while still proving the enum
    // constraint reaches the real generated document, not just the Scala-side
    // Validator/Schema values.
    assert(
      doc.noSpaces.contains(
        """"status":{"type":"string","enum":["created","updated","deleted"]}"""
      ),
      s"expected the generated doc to constrain status to the enum, got: $doc"
    )
  }
}
