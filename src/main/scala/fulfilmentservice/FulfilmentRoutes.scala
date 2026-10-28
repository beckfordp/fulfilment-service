package fulfilmentservice

import cats.effect.Async
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.HttpRoutes
import org.typelevel.log4cats.StructuredLogger
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

import java.util.UUID

final case class TaskResponse(
    orderId: String,
    state: String,
    items: List[FulfilmentItem],
    shippingAddress: Address,
    trackingNo: Option[String],
    carrier: Option[String],
    failureReason: Option[String],
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

object TaskResponse {
  implicit val codec: Codec[TaskResponse] = deriveCodec

  def apply(task: FulfilmentTask): TaskResponse =
    TaskResponse(
      task.orderId,
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

final case class DispatchRequest(trackingNo: String, carrier: String)

object DispatchRequest {
  implicit val codec: Codec[DispatchRequest] = deriveCodec
}

final case class FailRequest(reason: String)

object FailRequest {
  implicit val codec: Codec[FailRequest] = deriveCodec
}

final case class SimulateRequest(
    forceFailure: Option[Boolean] = None,
    reason: Option[String] = None
)

object SimulateRequest {
  implicit val codec: Codec[SimulateRequest] = deriveCodec
}

final case class ErrorResponse(error: String)

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

/** Internal operator API for the gshop admin fulfilment screen (US-17/18), plus
  * a config-gated `simulate-next` for deterministic demos.
  */
object FulfilmentRoutes {

  private val notFoundVariant
      : EndpointOutput.OneOfVariant[FulfilmentFailure.NotFound.type] =
    oneOfVariant(
      statusCode(StatusCode.NotFound)
        .and(jsonBody[ErrorResponse])
        .map[FulfilmentFailure.NotFound.type](_ => FulfilmentFailure.NotFound)(
          _ => ErrorResponse("Fulfilment task not found")
        )
    )

  private val invalidTransitionVariant
      : EndpointOutput.OneOfVariant[FulfilmentFailure.InvalidTransition] =
    oneOfVariant(
      statusCode(StatusCode.Conflict)
        .and(jsonBody[ErrorResponse])
        .map[FulfilmentFailure.InvalidTransition](e =>
          FulfilmentFailure.InvalidTransition(e.error)
        )(e => ErrorResponse(e.message))
    )

  private val invalidRequestVariant
      : EndpointOutput.OneOfVariant[FulfilmentFailure.InvalidRequest] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(jsonBody[ErrorResponse])
        .map[FulfilmentFailure.InvalidRequest](e =>
          FulfilmentFailure.InvalidRequest(e.error)
        )(e => ErrorResponse(e.message))
    )

  private val errorOutput: EndpointOutput[FulfilmentFailure] =
    oneOf[FulfilmentFailure](
      notFoundVariant,
      invalidTransitionVariant,
      invalidRequestVariant
    )

  private val listEndpoint: PublicEndpoint[Option[
    String
  ], FulfilmentFailure, List[TaskResponse], Any] =
    endpoint.get
      .in("fulfilment-tasks")
      .in(query[Option[String]]("state"))
      .out(jsonBody[List[TaskResponse]])
      .errorOut(errorOutput)

  private def actionEndpoint(
      action: String
  ): PublicEndpoint[String, FulfilmentFailure, TaskResponse, Any] =
    endpoint.post
      .in("fulfilment-tasks" / path[String]("orderId") / action)
      .out(jsonBody[TaskResponse])
      .errorOut(errorOutput)

  private val dispatchEndpoint: PublicEndpoint[
    (String, DispatchRequest),
    FulfilmentFailure,
    TaskResponse,
    Any
  ] =
    endpoint.post
      .in("fulfilment-tasks" / path[String]("orderId") / "dispatch")
      .in(jsonBody[DispatchRequest])
      .out(jsonBody[TaskResponse])
      .errorOut(errorOutput)

  private val failEndpoint: PublicEndpoint[
    (String, FailRequest),
    FulfilmentFailure,
    TaskResponse,
    Any
  ] =
    endpoint.post
      .in("fulfilment-tasks" / path[String]("orderId") / "fail")
      .in(jsonBody[FailRequest])
      .out(jsonBody[TaskResponse])
      .errorOut(errorOutput)

  private val simulateEndpoint: PublicEndpoint[
    (String, SimulateRequest),
    FulfilmentFailure,
    TaskResponse,
    Any
  ] =
    endpoint.post
      .in("fulfilment-tasks" / path[String]("orderId") / "simulate-next")
      .in(jsonBody[SimulateRequest])
      .out(jsonBody[TaskResponse])
      .errorOut(errorOutput)

  private def generatedTrackingNo(): String =
    s"GLN-${UUID.randomUUID().toString.take(8).toUpperCase}"

  def serverEndpoints[F[_]: Async](
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
        case Left(error)  => Async[F].pure(Left(error))
        case Right(state) =>
          store.list(state).map(tasks => Right(tasks.map(TaskResponse(_))))
      }
    }

    val startPicking = actionEndpoint("start-picking").serverLogic[F] {
      orderId =>
        store.startPicking(orderId).flatMap(respond(Map("order_id" -> orderId)))
    }

    val pack = actionEndpoint("pack").serverLogic[F] { orderId =>
      store.pack(orderId).flatMap(respond(Map("order_id" -> orderId)))
    }

    val dispatch = dispatchEndpoint.serverLogic[F] { case (orderId, request) =>
      store
        .dispatch(orderId, request.trackingNo, request.carrier)
        .flatMap(respond(Map("order_id" -> orderId)))
    }

    val fail = failEndpoint.serverLogic[F] { case (orderId, request) =>
      store
        .fail(orderId, request.reason)
        .flatMap(respond(Map("order_id" -> orderId)))
    }

    val simulate = simulateEndpoint.serverLogic[F] { case (orderId, request) =>
      store.get(orderId).flatMap {
        case None       => Async[F].pure(Left(FulfilmentFailure.NotFound))
        case Some(task) =>
          val outcome: F[Either[FulfilmentFailure, FulfilmentTask]] =
            task.state match {
              case FulfilmentState.Pending => store.startPicking(orderId)
              case FulfilmentState.Picking => store.pack(orderId)
              case FulfilmentState.Packed  =>
                if (request.forceFailure.getOrElse(false))
                  store.fail(
                    orderId,
                    request.reason.getOrElse("Simulated dispatch failure")
                  )
                else
                  store.dispatch(orderId, generatedTrackingNo(), "DHL Express")
              case _ =>
                Async[F].pure(
                  Left(
                    FulfilmentFailure.InvalidTransition(
                      s"Order $orderId is already ${task.state.wire}"
                    )
                  )
                )
            }
          outcome.flatMap(
            respond(
              Map("order_id" -> orderId, "simulated" -> "true")
            )
          )
      }
    }

    List(list, startPicking, pack, dispatch, fail) ++
      (if (simulationEnabled) List(simulate) else Nil)
  }

  def routes[F[_]: Async](
      store: FulfilmentStore[F],
      simulationEnabled: Boolean,
      logger: StructuredLogger[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      serverEndpoints(store, simulationEnabled, logger)
    )
}
