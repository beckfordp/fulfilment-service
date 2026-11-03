package fulfilmentservice

import cats.effect.IO
import fulfilmentservice.StoreCommands._
import fulfilmentservice.domain.{FulfilmentState, OrderId}
import io.opentelemetry.sdk.metrics.data.MetricData
import munit.CatsEffectSuite
import org.typelevel.log4cats.testing.StructuredTestingLogger
import purerest.metrics.Metrics

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters._

class FulfilmentJourneyMetricsSuite extends CatsEffectSuite {

  private val item = FulfilmentItem("sku-1", "Watch", 1, 1000)
  private val address = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private def attrs(p: io.opentelemetry.sdk.metrics.data.PointData) =
    p.getAttributes.asMap.asScala.map((k, v) => k.getKey -> v.toString).toMap

  private def counter(
      metrics: List[MetricData],
      name: String
  ): Map[Map[String, String], Long] =
    metrics
      .find(_.getName == name)
      .map(
        _.getLongSumData.getPoints.asScala
          .map(p => attrs(p) -> p.getValue)
          .toMap
      )
      .getOrElse(Map.empty)

  test(
    "counts new tasks and transitions, records step durations, logs milestones"
  ) {
    Metrics.test[IO]("fulfilment-journey").use { tm =>
      val logger = StructuredTestingLogger.impl[IO]()
      val orderId = OrderId.fromUuid(UUID.randomUUID())
      for {
        underlying <- FulfilmentStore.inMemory[IO]
        store <- FulfilmentJourneyMetrics
          .instrument[IO](underlying, tm.meter, logger)
        _ <- store.createTask(orderId, List(item), address)
        _ <- store.createTask(orderId, List(item), address)
        _ <- store.startPicking(orderId)
        _ <- store.pack(orderId)
        _ <- store.dispatch(orderId, "GLN-1", "DHL")
        rejected <- store.pack(orderId)
        metrics <- tm.collectMetrics
        logged <- logger.logged
      } yield {
        assert(rejected.isLeft)
        assertEquals(
          counter(metrics, "fulfilment.tasks.created").values.sum,
          1L
        )
        assertEquals(
          counter(metrics, "fulfilment.transitions"),
          Map(
            Map("from" -> "PENDING", "to" -> "PICKING") -> 1L,
            Map("from" -> "PICKING", "to" -> "PACKED") -> 1L,
            Map("from" -> "PACKED", "to" -> "DISPATCHED") -> 1L
          )
        )
        val steps = metrics
          .find(_.getName == "fulfilment.step.duration")
          .get
          .getHistogramData
          .getPoints
          .asScala
        assertEquals(steps.map(_.getCount).sum, 3L)
        assertEquals(
          logged.toList.flatMap(_.ctx.get("milestone")),
          List(
            "fulfilment_task_created",
            "fulfilment_picking",
            "fulfilment_packed",
            "order_dispatched"
          )
        )
      }
    }
  }

  test("backlog gauges report every open state, zero when empty") {
    Metrics.test[IO]("fulfilment-backlog").use { tm =>
      for {
        now <- IO.realTimeInstant
        rows = List(
          BacklogState(FulfilmentState.Pending, 4, Some(now.minusSeconds(300)))
        )
        metrics <- BacklogGauges
          .register[IO](tm.meter)(IO.pure(rows))
          .surround(tm.collectMetrics)
      } yield {
        val sizes = metrics
          .find(_.getName == "fulfilment.backlog.size")
          .get
          .getLongGaugeData
          .getPoints
          .asScala
          .map(p => attrs(p)("state") -> p.getValue)
          .toMap
        assertEquals(
          sizes,
          Map("PENDING" -> 4L, "PICKING" -> 0L, "PACKED" -> 0L)
        )
        val ages = metrics
          .find(_.getName == "fulfilment.backlog.oldest_age")
          .get
          .getDoubleGaugeData
          .getPoints
          .asScala
          .map(p => attrs(p)("state") -> p.getValue)
          .toMap
        assert(ages("PENDING") >= 300.0 && ages("PENDING") < 310.0, ages)
        assertEquals(ages("PACKED"), 0.0)
      }
    }
  }

  test("fromTasks groups open tasks by state with the oldest updatedAt") {
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    def task(state: FulfilmentState, at: Instant) =
      FulfilmentTask(
        OrderId.fromUuid(UUID.randomUUID()),
        state,
        List(item),
        address,
        None,
        None,
        None,
        at,
        at
      )
    assertEquals(
      BacklogGauges.fromTasks(
        List(
          task(FulfilmentState.Pending, t0.plusSeconds(5)),
          task(FulfilmentState.Pending, t0),
          task(FulfilmentState.Dispatched, t0.minusSeconds(5))
        )
      ),
      List(BacklogState(FulfilmentState.Pending, 2, Some(t0)))
    )
  }
}
