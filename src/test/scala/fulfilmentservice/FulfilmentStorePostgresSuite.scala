package fulfilmentservice

import fulfilmentservice.domain.{FulfilmentFailure, FulfilmentState}
import fulfilmentservice.StoreCommands._

import cats.effect.IO
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fulfilmentservice.domain.OrderId
import munit.CatsEffectSuite

import java.util.UUID
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.metrics.Meter
import purerest.metrics.{LatencyBuckets, Metrics}

import scala.jdk.CollectionConverters._

import java.sql.DriverManager

class FulfilmentStorePostgresSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(dockerImageName =
      DockerImageName.parse("postgres:16-alpine")
    )

  private val item = FulfilmentItem("sku-1", "Watch", 1, 1000)
  private val address = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private def configFor(postgres: PostgreSQLContainer): PostgresConfig =
    PostgresConfig(
      host = postgres.host,
      port = postgres.mappedPort(5432),
      database = postgres.databaseName,
      user = postgres.username,
      password = postgres.password
    )

  private def withStore[A](postgres: PostgreSQLContainer)(
      f: FulfilmentStore[IO] => IO[A]
  ): IO[A] =
    Migrations.run[IO](configFor(postgres)) *>
      FulfilmentStore
        .postgres[IO](configFor(postgres), Meter.noop[IO])
        .use(f)

  private def query(
      postgres: PostgreSQLContainer,
      sql: String
  ): IO[List[String]] =
    IO.blocking {
      val conn = DriverManager.getConnection(
        postgres.jdbcUrl,
        postgres.username,
        postgres.password
      )
      try {
        val rs = conn.createStatement().executeQuery(sql)
        Iterator
          .unfold(())(_ => if (rs.next()) Some((rs.getString(1), ())) else None)
          .toList
      } finally conn.close()
    }

  private def id(): OrderId = OrderId.fromUuid(UUID.randomUUID())

  test("backlog query groups open tasks by state with the oldest time") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        BacklogGauges.postgresQuery[IO](configFor(postgres)).use { backlog =>
          val a = OrderId.fromUuid(UUID.randomUUID())
          val b = OrderId.fromUuid(UUID.randomUUID())
          for {
            before <- backlog
            _ <- store.createTask(a, List(item), address)
            _ <- store.createTask(b, List(item), address)
            _ <- store.startPicking(b)
            after <- backlog
          } yield {
            // The container is shared across tests: assert the delta.
            def size(rows: List[BacklogState], s: FulfilmentState) =
              rows.find(_.state == s).fold(0L)(_.size)
            assertEquals(
              size(after, FulfilmentState.Pending) -
                size(before, FulfilmentState.Pending),
              1L
            )
            assertEquals(
              size(after, FulfilmentState.Picking) -
                size(before, FulfilmentState.Picking),
              1L
            )
            assert(after.forall(_.oldestEnteredAt.isDefined), after)
          }
        }
      }
    }
  }

  test("createTask is idempotent and persists the snapshot") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        val orderId = id()
        for {
          first <- store.createTask(orderId, List(item), address)
          second <- store.createTask(orderId, List(item), address)
          rows <- query(
            postgres,
            s"select count(*) from fulfilment_task where order_id = '$orderId'"
          )
        } yield {
          assertEquals(first, second)
          assertEquals(first.items, List(item))
          assertEquals(first.shippingAddress, address)
          assertEquals(rows, List("1"))
        }
      }
    }
  }

  test("transitions persist and dispatch writes the outbox row") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        val orderId = id()
        for {
          _ <- store.createTask(orderId, List(item), address)
          _ <- store.startPicking(orderId)
          _ <- store.pack(orderId)
          dispatched <- store.dispatch(orderId, "GLN-7", "DHL")
          fetched <- store.get(orderId)
          outbox <- query(
            postgres,
            s"select payload->>'eventType' from outbox_events where aggregate_id = '$orderId'"
          )
        } yield {
          assertEquals(
            dispatched.map(_.state),
            Right(FulfilmentState.Dispatched)
          )
          assertEquals(fetched.flatMap(_.trackingNo), Some("GLN-7"))
          assertEquals(outbox, List("DispatchConfirmed"))
        }
      }
    }
  }

  test("fail from PENDING persists the reason and writes DispatchFailed") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        val orderId = id()
        for {
          _ <- store.createTask(orderId, List(item), address)
          failed <- store.fail(orderId, "damaged")
          outbox <- query(
            postgres,
            s"select payload->>'eventType' from outbox_events where aggregate_id = '$orderId'"
          )
        } yield {
          assertEquals(
            failed.map(_.state),
            Right(FulfilmentState.DispatchFailed)
          )
          assertEquals(outbox, List("DispatchFailed"))
        }
      }
    }
  }

  test("cancel persists CANCELLED and writes no fulfilment event") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        val orderId = id()
        for {
          _ <- store.createTask(orderId, List(item), address)
          cancelled <- store.cancel(orderId)
          fetched <- store.get(orderId)
          outbox <- query(
            postgres,
            s"select payload->>'eventType' from outbox_events where aggregate_id = '$orderId'"
          )
        } yield {
          assertEquals(
            cancelled.map(_.state),
            Right(FulfilmentState.Cancelled)
          )
          assertEquals(fetched.map(_.state), Some(FulfilmentState.Cancelled))
          assertEquals(outbox, Nil)
        }
      }
    }
  }

  test("cancel after dispatch is rejected with from=DISPATCHED") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        val orderId = id()
        for {
          _ <- store.createTask(orderId, List(item), address)
          _ <- store.startPicking(orderId)
          _ <- store.pack(orderId)
          _ <- store.dispatch(orderId, "GLN-7", "DHL")
          rejected <- store.cancel(orderId)
          fetched <- store.get(orderId)
        } yield {
          assertEquals(
            rejected,
            Left(
              FulfilmentFailure.InvalidTransition(
                s"Cannot transition order ${OrderId.render(orderId)} from DISPATCHED",
                Some(FulfilmentState.Dispatched)
              )
            )
          )
          assertEquals(fetched.map(_.state), Some(FulfilmentState.Dispatched))
        }
      }
    }
  }

  test("cancel racing dispatch has exactly one winner") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        val orderId = id()
        for {
          _ <- store.createTask(orderId, List(item), address)
          _ <- store.startPicking(orderId)
          _ <- store.pack(orderId)
          results <- IO.both(
            store.cancel(orderId),
            store.dispatch(orderId, "GLN-8", "DHL")
          )
          fetched <- store.get(orderId)
          outbox <- query(
            postgres,
            s"select payload->>'eventType' from outbox_events where aggregate_id = '$orderId'"
          )
        } yield {
          assertEquals(
            List(results._1.isRight, results._2.isRight).count(identity),
            1
          )
          val dispatchWon = results._2.isRight
          assertEquals(
            fetched.map(_.state),
            Some(
              if (dispatchWon) FulfilmentState.Dispatched
              else FulfilmentState.Cancelled
            )
          )
          assertEquals(
            outbox.count(_ == "DispatchConfirmed"),
            if (dispatchWon) 1 else 0
          )
        }
      }
    }
  }

  test("reconciliation flags are first-wins and require a task") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        val orderId = id()
        for {
          _ <- store.createTask(orderId, List(item), address)
          first <- store.flagReconciliation(orderId, "first")
          second <- store.flagReconciliation(orderId, "second")
          reason <- store.reconciliationReason(orderId)
          unknown <- store.flagReconciliation(id(), "x")
        } yield {
          assert(first)
          assert(!second)
          assertEquals(reason, Some("first"))
          assert(!unknown)
        }
      }
    }
  }

  test("invalid transitions and unknown orders are rejected") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        val orderId = id()
        for {
          _ <- store.createTask(orderId, List(item), address)
          invalid <- store.pack(orderId)
          missing <- store.startPicking(id())
        } yield {
          assert(
            invalid.left.exists(
              _.isInstanceOf[FulfilmentFailure.InvalidTransition]
            )
          )
          assertEquals(missing, Left(FulfilmentFailure.NotFound))
        }
      }
    }
  }

  test("list filters by state") {
    withContainers { postgres =>
      withStore(postgres) { store =>
        val pending = id()
        val picking = id()
        for {
          _ <- store.createTask(pending, List(item), address)
          _ <- store.createTask(picking, List(item), address)
          _ <- store.startPicking(picking)
          pendingRows <- store.list(Some(FulfilmentState.Pending))
          pickingRows <- store.list(Some(FulfilmentState.Picking))
        } yield {
          assert(pendingRows.exists(_.orderId == pending))
          assert(!pendingRows.exists(_.orderId == picking))
          assert(pickingRows.exists(_.orderId == picking))
          assert(!pickingRows.exists(_.orderId == pending))
        }
      }
    }
  }

  test("db.client.operation.duration uses LatencyBuckets.seconds boundaries") {
    withContainers { postgres =>
      Metrics.test[IO]("fulfilment-store-buckets-test").use { testMeter =>
        Migrations.run[IO](configFor(postgres)) *>
          FulfilmentStore
            .postgres[IO](configFor(postgres), testMeter.meter)
            .use { store =>
              for {
                _ <- store.ping
                metrics <- testMeter.collectMetrics
              } yield {
                val points = metrics
                  .filter(_.getName == "db.client.operation.duration")
                  .flatMap(_.getHistogramData.getPoints.asScala)
                assert(
                  points.nonEmpty,
                  s"expected db.client.operation.duration points, got: $metrics"
                )
                points.foreach { point =>
                  assertEquals(
                    point.getBoundaries.asScala.map(_.doubleValue).toVector,
                    LatencyBuckets.seconds.boundaries
                  )
                }
              }
            }
      }
    }
  }
}
