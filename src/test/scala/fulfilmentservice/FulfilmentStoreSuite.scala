package fulfilmentservice

import cats.effect.IO
import fulfilmentservice.domain.OrderId
import munit.CatsEffectSuite

import java.util.UUID

class FulfilmentStoreSuite extends CatsEffectSuite {

  private val orderId: OrderId = OrderId.fromUuid(UUID.randomUUID())
  private val otherOrderId: OrderId = OrderId.fromUuid(UUID.randomUUID())

  private val item = FulfilmentItem("sku-1", "Watch", 1, 1000)
  private val address = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private def withStore[A](
      f: (
          FulfilmentStore[IO],
          cats.effect.Ref[IO, List[(String, EventEnvelope)]]
      ) => IO[A]
  ): IO[A] =
    FulfilmentStore.inMemoryWithOutbox[IO].flatMap { case (store, outbox) =>
      f(store, outbox)
    }

  test("createTask is idempotent per orderId") {
    withStore { (store, _) =>
      for {
        first <- store.createTask(orderId, List(item), address)
        second <- store.createTask(orderId, List(item), address)
        all <- store.list(None)
      } yield {
        assertEquals(first.state, FulfilmentState.Pending)
        assertEquals(second, first)
        assertEquals(all.size, 1)
      }
    }
  }

  test(
    "drives PENDING -> PICKING -> PACKED -> DISPATCHED and emits one event"
  ) {
    withStore { (store, outbox) =>
      for {
        _ <- store.createTask(orderId, List(item), address)
        picking <- store.startPicking(orderId)
        packed <- store.pack(orderId)
        dispatched <- store.dispatch(orderId, " GLN-1 ", "DHL")
        events <- outbox.get
      } yield {
        assertEquals(picking.map(_.state), Right(FulfilmentState.Picking))
        assertEquals(packed.map(_.state), Right(FulfilmentState.Packed))
        assertEquals(
          dispatched.map(_.state),
          Right(FulfilmentState.Dispatched)
        )
        assertEquals(
          dispatched.toOption.flatMap(_.trackingNo),
          Some("GLN-1")
        )
        assertEquals(events.size, 1)
        assertEquals(events.head._2.eventType, "DispatchConfirmed")
      }
    }
  }

  test("fail is allowed from any non-terminal state and emits DispatchFailed") {
    withStore { (store, outbox) =>
      for {
        _ <- store.createTask(orderId, List(item), address)
        failed <- store.fail(orderId, " damaged ")
        events <- outbox.get
      } yield {
        assertEquals(
          failed.map(_.state),
          Right(FulfilmentState.DispatchFailed)
        )
        assertEquals(failed.toOption.flatMap(_.failureReason), Some("damaged"))
        assertEquals(events.head._2.eventType, "DispatchFailed")
      }
    }
  }

  test("rejects invalid and terminal transitions") {
    withStore { (store, _) =>
      for {
        _ <- store.createTask(orderId, List(item), address)
        invalid <- store.pack(orderId)
        _ <- store.fail(orderId, "gone")
        terminal <- store.startPicking(orderId)
        missing <- store.startPicking(otherOrderId)
      } yield {
        assert(
          invalid.left.exists(
            _.isInstanceOf[FulfilmentFailure.InvalidTransition]
          )
        )
        assert(
          terminal.left.exists(
            _.isInstanceOf[FulfilmentFailure.InvalidTransition]
          )
        )
        assertEquals(missing, Left(FulfilmentFailure.NotFound))
      }
    }
  }

  test("dispatch and fail validate their required fields") {
    withStore { (store, _) =>
      for {
        _ <- store.createTask(orderId, List(item), address)
        _ <- store.startPicking(orderId)
        _ <- store.pack(orderId)
        noTracking <- store.dispatch(orderId, " ", "DHL")
        noReason <- store.fail(orderId, "  ")
      } yield {
        assertEquals(
          noTracking,
          Left(
            FulfilmentFailure.InvalidRequest(
              "trackingNo and carrier are required"
            )
          )
        )
        assertEquals(
          noReason,
          Left(FulfilmentFailure.InvalidRequest("reason is required"))
        )
      }
    }
  }

  test("list filters by state") {
    withStore { (store, _) =>
      for {
        _ <- store.createTask(orderId, List(item), address)
        _ <- store.createTask(otherOrderId, List(item), address)
        _ <- store.startPicking(otherOrderId)
        pending <- store.list(Some(FulfilmentState.Pending))
        picking <- store.list(Some(FulfilmentState.Picking))
      } yield {
        assertEquals(pending.map(_.orderId), List(orderId))
        assertEquals(picking.map(_.orderId), List(otherOrderId))
      }
    }
  }
}
