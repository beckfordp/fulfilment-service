package fulfilmentservice

import cats.{Functor, MonadThrow}
import cats.effect.Async
import cats.effect.std.UUIDGen
import cats.syntax.all._
import io.circe.Codec
import fulfilmentservice.domain.{
  FulfilmentCommand,
  FulfilmentFailure,
  FulfilmentLifecycle,
  FulfilmentState,
  OrderId
}
import io.circe.generic.semiauto.deriveCodec
import org.http4s.HttpRoutes
import org.typelevel.log4cats.StructuredLogger
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.Schema.annotations.{description, encodedExample}
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

import java.time.Instant
import java.util.UUID

@description(
  "A fulfilment task as stored: order snapshot, lifecycle state and dispatch outcome."
)
final case class TaskResponse(
    @description("Order id (UUID) from order-service.")
    @encodedExample("3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90")
    orderId: String,
    @description("Lifecycle state.") @encodedExample("PICKING") state: String,
    @description("Order lines snapshotted when the task was created.")
    items: List[FulfilmentItem],
    @description("Where the order ships.") shippingAddress: Address,
    @description("Carrier tracking number; present once DISPATCHED.")
    @encodedExample("GLN-1A2B3C4D")
    trackingNo: Option[String],
    @description("Carrier name; present once DISPATCHED.")
    @encodedExample("DHL Express")
    carrier: Option[String],
    @description("Why dispatch failed; present once DISPATCH_FAILED.")
    @encodedExample("carrier_unavailable")
    failureReason: Option[String],
    @description("When the task was created (UTC).")
    @encodedExample("2026-10-31T10:15:30Z")
    createdAt: Instant,
    @description("When the task last changed (UTC).")
    @encodedExample("2026-10-31T10:20:00Z")
    updatedAt: Instant
)

object TaskResponse {
  implicit val codec: Codec[TaskResponse] = deriveCodec

  def apply(task: FulfilmentTask): TaskResponse =
    TaskResponse(
      OrderId.render(task.orderId),
      task.state.wire,
      task.items,
      task.shippingAddress,
      task.trackingNo,
      task.carrier,
      task.failureReason,
      task.createdAt,
      task.updatedAt
    )
}

@description(
  "Dispatch details: who is carrying the order and its tracking number."
)
final case class DispatchRequest(
    @description("Carrier tracking number; non-blank.")
    @encodedExample("GLN-1A2B3C4D")
    trackingNo: String,
    @description("Carrier name; non-blank.") @encodedExample("DHL Express")
    carrier: String
)

object DispatchRequest {
  implicit val codec: Codec[DispatchRequest] = deriveCodec
}

@description("Why the task could not be dispatched.")
final case class FailRequest(
    @description("Failure reason; non-blank.")
    @encodedExample("carrier_unavailable")
    reason: String
)

object FailRequest {
  implicit val codec: Codec[FailRequest] = deriveCodec
}

@description(
  "Demo simulation options; both optional (send `{}` to advance one step)."
)
final case class SimulateRequest(
    @description("Fail the task instead of advancing it.")
    @encodedExample(true)
    forceFailure: Option[Boolean] = None,
    @description(
      "Failure reason when `forceFailure` is true; defaults to `Simulated dispatch failure`."
    )
    @encodedExample("simulated dispatch failure")
    reason: Option[String] = None
)

object SimulateRequest {
  implicit val codec: Codec[SimulateRequest] = deriveCodec
}

@description("Error body.")
final case class ErrorResponse(
    @description("Human-readable reason.")
    @encodedExample("Fulfilment task not found")
    error: String
)

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

/** Internal operator API for the gshop admin fulfilment screen (US-17/18), plus
  * a config-gated `simulate-next` for deterministic demos.
  */
object FulfilmentRoutes {

  private val Tag = "fulfilment-tasks"

  private val exampleOrderId: OrderId =
    OrderId.fromUuid(UUID.fromString("3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90"))

  private val exampleAddress: Address =
    Address("1 High Street", Some("Flat 2"), "London", "SW1A 1AA", "GB")

  private val exampleTask: TaskResponse =
    TaskResponse(
      OrderId.render(exampleOrderId),
      "DISPATCHED",
      List(FulfilmentItem("SKU-0042", "Espresso cup", 2, 1250)),
      exampleAddress,
      Some("GLN-1A2B3C4D"),
      Some("DHL Express"),
      None,
      Instant.parse("2026-10-31T10:15:30Z"),
      Instant.parse("2026-10-31T10:20:00Z")
    )

  private val notFoundBody: ErrorResponse =
    ErrorResponse("Fulfilment task not found")

  private def errorBody(
      what: String,
      example: ErrorResponse
  ): EndpointIO.Body[String, ErrorResponse] =
    jsonBody[ErrorResponse].description(what).example(example)

  private val notFoundVariant
      : EndpointOutput.OneOfVariant[FulfilmentFailure.NotFound.type] =
    oneOfVariant(
      statusCode(StatusCode.NotFound)
        .and(errorBody("No task exists for this order id.", notFoundBody))
        .map[FulfilmentFailure.NotFound.type](_ => FulfilmentFailure.NotFound)(
          _ => notFoundBody
        )
    )

  private val invalidTransitionVariant
      : EndpointOutput.OneOfVariant[FulfilmentFailure.InvalidTransition] =
    oneOfVariant(
      statusCode(StatusCode.Conflict)
        .and(
          errorBody(
            "The task can't move from its current state (wrong state for this action, or already terminal).",
            ErrorResponse(
              "Cannot transition order 3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90 from PENDING"
            )
          )
        )
        .map[FulfilmentFailure.InvalidTransition](e =>
          FulfilmentFailure.InvalidTransition(e.error)
        )(e => ErrorResponse(e.message))
    )

  private val invalidRequestVariant
      : EndpointOutput.OneOfVariant[FulfilmentFailure.InvalidRequest] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(
          errorBody(
            "Invalid request input: an unknown `state` filter, or a blank `trackingNo`/`carrier`/`reason`.",
            ErrorResponse("Unknown state: SHIPPED")
          )
        )
        .map[FulfilmentFailure.InvalidRequest](e =>
          FulfilmentFailure.InvalidRequest(e.error)
        )(e => ErrorResponse(e.message))
    )

  private val notFoundOrInvalidTransitionOutput
      : EndpointOutput[FulfilmentFailure] =
    oneOf[FulfilmentFailure](notFoundVariant, invalidTransitionVariant)

  private val invalidRequestOutput: EndpointOutput[FulfilmentFailure] =
    oneOf[FulfilmentFailure](invalidRequestVariant)

  private val fullErrorOutput: EndpointOutput[FulfilmentFailure] =
    oneOf[FulfilmentFailure](
      notFoundVariant,
      invalidTransitionVariant,
      invalidRequestVariant
    )

  private val orderIdPath: EndpointInput.PathCapture[OrderId] =
    path[OrderId]("orderId")
      .description(
        "Order id (UUID) the task was created for. A malformed id returns 400."
      )
      .example(exampleOrderId)

  private val listEndpoint: PublicEndpoint[Option[
    String
  ], FulfilmentFailure, List[TaskResponse], Any] =
    endpoint.get
      .in("fulfilment-tasks")
      .in(
        query[Option[String]]("state")
          .description(
            "Filter by lifecycle state: `PENDING`, `PICKING`, `PACKED`, `DISPATCHED` or `DISPATCH_FAILED`."
          )
          .example(Some("PENDING"))
      )
      .out(jsonBody[List[TaskResponse]].example(List(exampleTask)))
      .errorOut(invalidRequestOutput)
      .summary("List fulfilment tasks")
      .description(
        "Every pick/pack task, oldest first (createdAt). Optional `state` filter; an unknown value is 400. Read directly from Postgres — no cache, so results are always current."
      )
      .tag(Tag)

  private def actionEndpoint(
      action: String,
      summary: String,
      description: String
  ): PublicEndpoint[OrderId, FulfilmentFailure, TaskResponse, Any] =
    endpoint.post
      .in("fulfilment-tasks" / orderIdPath / action)
      .out(jsonBody[TaskResponse].example(exampleTask))
      .errorOut(notFoundOrInvalidTransitionOutput)
      .summary(summary)
      .description(description)
      .tag(Tag)

  private val dispatchEndpoint: PublicEndpoint[
    (OrderId, DispatchRequest),
    FulfilmentFailure,
    TaskResponse,
    Any
  ] =
    endpoint.post
      .in("fulfilment-tasks" / orderIdPath / "dispatch")
      .in(
        jsonBody[DispatchRequest]
          .example(DispatchRequest("GLN-1A2B3C4D", "DHL Express"))
      )
      .out(jsonBody[TaskResponse].example(exampleTask))
      .errorOut(fullErrorOutput)
      .summary("Dispatch a task")
      .description(
        "Moves the task PACKED → DISPATCHED, records the tracking number and carrier, and publishes `DispatchConfirmed` through the transactional outbox. Blank `trackingNo`/`carrier` → 400. 404 if no task exists; 409 from any other state. Not idempotent — a repeat is 409."
      )
      .tag(Tag)

  private val failEndpoint: PublicEndpoint[
    (OrderId, FailRequest),
    FulfilmentFailure,
    TaskResponse,
    Any
  ] =
    endpoint.post
      .in("fulfilment-tasks" / orderIdPath / "fail")
      .in(
        jsonBody[FailRequest].example(FailRequest("carrier_unavailable"))
      )
      .out(jsonBody[TaskResponse].example(exampleTask))
      .errorOut(fullErrorOutput)
      .summary("Fail a task")
      .description(
        "Moves the task to DISPATCH_FAILED from PENDING, PICKING or PACKED, records the reason, and publishes `DispatchFailed` through the transactional outbox. Blank `reason` → 400. 404 if no task exists; 409 from a terminal state."
      )
      .tag(Tag)

  private val simulateEndpoint: PublicEndpoint[
    (OrderId, SimulateRequest),
    FulfilmentFailure,
    TaskResponse,
    Any
  ] =
    endpoint.post
      .in("fulfilment-tasks" / orderIdPath / "simulate-next")
      .in(
        jsonBody[SimulateRequest].example(
          SimulateRequest(Some(true), Some("simulated dispatch failure"))
        )
      )
      .out(jsonBody[TaskResponse].example(exampleTask))
      .errorOut(notFoundOrInvalidTransitionOutput)
      .summary("Advance a task one step (simulation)")
      .description(
        "Demo-only; registered only when `fulfilment.simulation.enabled`. Advances PENDING → PICKING → PACKED → DISPATCHED, minting a `GLN-XXXXXXXX` tracking number and carrier `DHL Express`; with `forceFailure` it fails the task instead. 404 if no task exists; 409 from a terminal state."
      )
      .tag(Tag)

  private val simulatedCarrier: String = "DHL Express"

  private def generatedTrackingNo[F[_]: Functor: UUIDGen]: F[String] =
    UUIDGen[F].randomUUID.map(id => s"GLN-${id.toString.take(8).toUpperCase}")

  def serverEndpoints[F[_]: MonadThrow: UUIDGen](
      store: FulfilmentStore[F],
      simulationEnabled: Boolean,
      logger: StructuredLogger[F]
  ): List[ServerEndpoint[Any, F]] = {
    def log(context: Map[String, String])(message: String): F[Unit] =
      logger.info(context)(message)

    def respond(
        context: Map[String, String]
    )(
        result: Either[FulfilmentFailure, FulfilmentTask]
    ): F[Either[FulfilmentFailure, TaskResponse]] =
      result match {
        case Right(task) =>
          log(context + ("state" -> task.state.wire))("Fulfilment task updated")
            .as(Right(TaskResponse(task)))
        case Left(error) =>
          log(context + ("error" -> error.toString))("Fulfilment task rejected")
            .as(Left(error))
      }

    val list = listEndpoint.serverLogic[F] { stateFilter =>
      val parsed = stateFilter match {
        case None        => Right(None)
        case Some(value) =>
          FulfilmentState
            .fromString(value)
            .toRight(FulfilmentFailure.InvalidRequest(s"Unknown state: $value"))
            .map(Some(_))
      }
      parsed match {
        case Left(error)  => MonadThrow[F].pure(Left(error))
        case Right(state) =>
          store.list(state).map(tasks => Right(tasks.map(TaskResponse(_))))
      }
    }

    def run(orderId: OrderId, context: Map[String, String])(
        command: Either[FulfilmentFailure, FulfilmentCommand]
    ): F[Either[FulfilmentFailure, TaskResponse]] =
      command
        .traverse(store.transition(orderId, _))
        .map(_.flatten)
        .flatMap(respond(context))

    def orderContext(orderId: OrderId): Map[String, String] =
      Map("order_id" -> OrderId.render(orderId))

    val startPicking = actionEndpoint(
      "start-picking",
      "Start picking a task",
      "Moves the task PENDING → PICKING. No event published. 404 if no task exists; 409 from any other state (so repeating it while PICKING is a 409)."
    ).serverLogic[F] { orderId =>
      run(orderId, orderContext(orderId))(
        Right(FulfilmentCommand.StartPicking)
      )
    }

    val pack = actionEndpoint(
      "pack",
      "Mark a task packed",
      "Moves the task PICKING → PACKED. No event published. 404 if no task exists; 409 from any other state."
    ).serverLogic[F] { orderId =>
      run(orderId, orderContext(orderId))(Right(FulfilmentCommand.Pack))
    }

    val dispatch = dispatchEndpoint.serverLogic[F] { case (orderId, request) =>
      run(orderId, orderContext(orderId))(
        FulfilmentCommand.dispatch(request.trackingNo, request.carrier)
      )
    }

    val fail = failEndpoint.serverLogic[F] { case (orderId, request) =>
      run(orderId, orderContext(orderId))(
        FulfilmentCommand.fail(request.reason)
      )
    }

    val simulate = simulateEndpoint.serverLogic[F] { case (orderId, request) =>
      store.get(orderId).flatMap {
        case None       => MonadThrow[F].pure(Left(FulfilmentFailure.NotFound))
        case Some(task) =>
          val command: F[Either[FulfilmentFailure, FulfilmentCommand]] =
            if (request.forceFailure.getOrElse(false))
              MonadThrow[F].pure(
                FulfilmentCommand.fail(
                  request.reason.getOrElse("Simulated dispatch failure")
                )
              )
            else
              generatedTrackingNo[F].map(trackingNo =>
                FulfilmentCommand
                  .dispatch(trackingNo, simulatedCarrier)
                  .flatMap(dispatch =>
                    FulfilmentLifecycle
                      .simulatedNext(task.state, dispatch)
                      .toRight(
                        FulfilmentFailure.InvalidTransition(
                          s"Order ${OrderId.render(orderId)} is already ${task.state.wire}"
                        )
                      )
                  )
              )
          command.flatMap(
            run(orderId, orderContext(orderId) + ("simulated" -> "true"))
          )
      }
    }

    List(list, startPicking, pack, dispatch, fail) ++
      (if (simulationEnabled) List(simulate) else Nil)
  }

  def routes[F[_]: Async: UUIDGen](
      store: FulfilmentStore[F],
      simulationEnabled: Boolean,
      logger: StructuredLogger[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      serverEndpoints(store, simulationEnabled, logger)
    )
}
