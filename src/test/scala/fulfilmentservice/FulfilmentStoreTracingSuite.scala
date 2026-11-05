package fulfilmentservice

import cats.effect.IO
import fulfilmentservice.StoreCommands._
import fulfilmentservice.domain.OrderId
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.trace.data.SpanData
import munit.CatsEffectSuite
import purerest.tracing.Tracing

import java.util.UUID
import scala.jdk.CollectionConverters._

class FulfilmentStoreTracingSuite extends CatsEffectSuite {

  private val item = FulfilmentItem("sku-1", "Watch", 1, 1000)
  private val address = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private def attr(span: SpanData, key: String): Option[String] =
    Option(span.getAttributes.get(AttributeKey.stringKey(key)))

  test(
    "create and transitions run in business spans under the caller's span, with status_changed events"
  ) {
    Tracing.test[IO]("fulfilment-store-tracing").use { tt =>
      val orderId = OrderId.fromUuid(UUID.randomUUID())
      for {
        underlying <- FulfilmentStore.inMemory[IO]
        store = FulfilmentStoreTracing.instrument[IO](underlying, tt.tracer)
        parentId <- tt.tracer.span("request").use { span =>
          store.createTask(orderId, List(item), address) *>
            store.startPicking(orderId) *>
            store.pack(orderId) *>
            store.dispatch(orderId, "GLN-1", "DHL") *>
            store.pack(orderId) *>
            IO.pure(span.context.spanIdHex)
        }
        spans <- tt.finishedSpans
      } yield {
        val children = spans.filter(_.getParentSpanId == parentId)
        children.foreach(s =>
          assertEquals(attr(s, "order.id"), Some(OrderId.render(orderId)))
        )
        assertEquals(
          children.map(s =>
            (
              s.getName,
              attr(s, "fulfilment.command"),
              s.getEvents.asScala.toList.map(e =>
                Option(e.getAttributes.get(AttributeKey.stringKey("to")))
              ),
              attr(s, "outcome")
            )
          ),
          List[(String, Option[String], List[Option[String]], Option[String])](
            ("fulfilment.create_task", None, Nil, None),
            (
              "fulfilment.transition",
              Some("StartPicking"),
              List(Some("PICKING")),
              None
            ),
            ("fulfilment.transition", Some("Pack"), List(Some("PACKED")), None),
            (
              "fulfilment.transition",
              Some("Dispatch"),
              List(Some("DISPATCHED")),
              None
            ),
            ("fulfilment.transition", Some("Pack"), Nil, Some("rejected"))
          )
        )
      }
    }
  }

  test("a store error marks the span Error") {
    Tracing.test[IO]("fulfilment-store-tracing-error").use { tt =>
      for {
        underlying <- FulfilmentStore.inMemory[IO]
        failing = new FulfilmentStore[IO] {
          export underlying.{createTask => _, *}
          def createTask(
              orderId: OrderId,
              items: List[FulfilmentItem],
              shippingAddress: Address
          ): IO[FulfilmentTask] =
            IO.raiseError(new RuntimeException("db down"))
        }
        result <- FulfilmentStoreTracing
          .instrument[IO](failing, tt.tracer)
          .createTask(OrderId.fromUuid(UUID.randomUUID()), List(item), address)
          .attempt
        spans <- tt.finishedSpans
      } yield {
        assert(result.isLeft)
        assertEquals(
          spans.map(s => (s.getName, s.getStatus.getStatusCode)),
          List(("fulfilment.create_task", StatusCode.ERROR))
        )
      }
    }
  }
}
