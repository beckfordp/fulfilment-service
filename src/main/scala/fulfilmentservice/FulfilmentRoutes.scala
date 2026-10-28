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

final case class CreateFulfilmentRequest( /* codegen:fields:CREATE_PARAMS */ )

object CreateFulfilmentRequest {
  implicit val codec: Codec[CreateFulfilmentRequest] = deriveCodec
}

final case class UpdateFulfilmentRequest( /* codegen:fields:UPDATE_PARAMS */ )

object UpdateFulfilmentRequest {
  implicit val codec: Codec[UpdateFulfilmentRequest] = deriveCodec
}

final case class FulfilmentResponse(
    id: String,
    // codegen:fields:CASE_CLASS_FIELD
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

object FulfilmentResponse {
  implicit val codec: Codec[FulfilmentResponse] = deriveCodec

  def apply(entity: Fulfilment): FulfilmentResponse =
    FulfilmentResponse(
      entity.id,
      /* codegen:fields:RESPONSE_APPLY_ARGS */
      entity.createdAt,
      entity.updatedAt
    )
}

final case class ErrorResponse(error: String)

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

object FulfilmentRoutes {

  private val createFulfilmentEndpoint: PublicEndpoint[
    CreateFulfilmentRequest,
    Unit,
    FulfilmentResponse,
    Any
  ] =
    endpoint.post
      .in("fulfilments")
      .in(jsonBody[CreateFulfilmentRequest])
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[FulfilmentResponse])

  private val notFoundOutput: EndpointOutput[FulfilmentError] =
    statusCode(StatusCode.NotFound)
      .and(jsonBody[ErrorResponse])
      .map[FulfilmentError](_ => FulfilmentNotFound)(_ =>
        ErrorResponse("Fulfilment not found")
      )

  private val invalidPaginationOutput: EndpointOutput[FulfilmentError] =
    statusCode(StatusCode.BadRequest)
      .and(jsonBody[ErrorResponse])
      .map[FulfilmentError](_ => InvalidPagination)(_ =>
        ErrorResponse("Invalid pagination parameters")
      )

  private val listFulfilmentsEndpoint: PublicEndpoint[
    (Option[Int], Option[Int]),
    FulfilmentError,
    (List[FulfilmentResponse], String),
    Any
  ] =
    endpoint.get
      .in("fulfilments")
      .in(query[Option[Int]]("limit"))
      .in(query[Option[Int]]("offset"))
      .out(jsonBody[List[FulfilmentResponse]])
      .out(header[String]("X-Total-Count"))
      .errorOut(invalidPaginationOutput)

  private val getFulfilmentEndpoint: PublicEndpoint[
    String,
    FulfilmentError,
    FulfilmentResponse,
    Any
  ] =
    endpoint.get
      .in("fulfilments" / path[String]("id"))
      .out(jsonBody[FulfilmentResponse])
      .errorOut(notFoundOutput)

  private val updateFulfilmentEndpoint: PublicEndpoint[
    (String, UpdateFulfilmentRequest),
    FulfilmentError,
    FulfilmentResponse,
    Any
  ] =
    endpoint.patch
      .in("fulfilments" / path[String]("id"))
      .in(jsonBody[UpdateFulfilmentRequest])
      .out(jsonBody[FulfilmentResponse])
      .errorOut(notFoundOutput)

  private val replaceFulfilmentEndpoint: PublicEndpoint[
    (String, UpdateFulfilmentRequest),
    FulfilmentError,
    FulfilmentResponse,
    Any
  ] =
    endpoint.put
      .in("fulfilments" / path[String]("id"))
      .in(jsonBody[UpdateFulfilmentRequest])
      .out(jsonBody[FulfilmentResponse])
      .errorOut(notFoundOutput)

  private val deleteFulfilmentEndpoint
      : PublicEndpoint[String, FulfilmentError, Unit, Any] =
    endpoint.delete
      .in("fulfilments" / path[String]("id"))
      .out(statusCode(StatusCode.NoContent))
      .errorOut(notFoundOutput)

  def serverEndpoint[F[_]: Async](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    createFulfilmentEndpoint.serverLogicSuccess[F] { req =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "POST",
            "path" -> "/fulfilments"
          )
        )("Received request")
        entity <- store
          .create( /* codegen:fields:CREATE_CALL_ARGS */ )
          .onError { case error =>
            logger.error(Map.empty, error)("Persisting the fulfilment failed")
          }
        _ <- logger.info(
          Map("fulfilment_id" -> entity.id)
        )("Request completed")
      } yield FulfilmentResponse(entity)
    }

  def getFulfilmentServerEndpoint[F[_]: Async](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    getFulfilmentEndpoint.serverLogic[F] { id =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "GET",
            "path" -> s"/fulfilments/$id",
            "fulfilment_id" -> id
          )
        )(
          "Received request"
        )
        result <- store.get(id).flatMap {
          case Some(entity) =>
            logger
              .info(Map("fulfilment_id" -> id))("Request completed")
              .as(Right(FulfilmentResponse(entity)))
          case None =>
            logger
              .warn(Map("fulfilment_id" -> id))("Fulfilment not found")
              .as(Left(FulfilmentNotFound))
        }
      } yield result
    }

  /** Shared handler for `PATCH` (partial update) and `PUT` (full replace) —
    * both call `FulfilmentStore.update` with the same required update body;
    * only the logged HTTP method differs.
    */
  private def updateLogic[F[_]: Async](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F],
      httpMethod: String
  )(
      id: String,
      req: UpdateFulfilmentRequest
  ): F[Either[FulfilmentError, FulfilmentResponse]] =
    for {
      _ <- logger.info(
        Map(
          "method" -> httpMethod,
          "path" -> s"/fulfilments/$id",
          "fulfilment_id" -> id
        )
      )("Received request")
      result <- store
        .update(id /* codegen:fields:UPDATE_CALL_ARGS */ )
        .flatMap {
          case Some(entity) =>
            logger
              .info(Map("fulfilment_id" -> id))("Request completed")
              .as(Right(FulfilmentResponse(entity)))
          case None =>
            logger
              .warn(Map("fulfilment_id" -> id))("Fulfilment not found")
              .as(Left(FulfilmentNotFound))
        }
    } yield result

  def updateFulfilmentServerEndpoint[F[_]: Async](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    updateFulfilmentEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PATCH")(id, req)
    }

  def replaceFulfilmentServerEndpoint[F[_]: Async](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    replaceFulfilmentEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PUT")(id, req)
    }

  def deleteFulfilmentServerEndpoint[F[_]: Async](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    deleteFulfilmentEndpoint.serverLogic[F] { id =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "DELETE",
            "path" -> s"/fulfilments/$id",
            "fulfilment_id" -> id
          )
        )("Received request")
        result <- store.delete(id).flatMap {
          case true =>
            logger
              .info(Map("fulfilment_id" -> id))("Request completed")
              .as(Right(()))
          case false =>
            logger
              .warn(Map("fulfilment_id" -> id))("Fulfilment not found")
              .as(Left(FulfilmentNotFound))
        }
      } yield result
    }

  private val defaultLimit = 20
  private val maxLimit = 100

  def listFulfilmentsServerEndpoint[F[_]: Async](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    listFulfilmentsEndpoint.serverLogic[F] { case (limitParam, offsetParam) =>
      val limit = limitParam.getOrElse(defaultLimit)
      val offset = offsetParam.getOrElse(0)
      for {
        _ <- logger.info(
          Map("method" -> "GET", "path" -> "/fulfilments")
        )("Received request")
        result <-
          if (limit <= 0 || limit > maxLimit || offset < 0) {
            logger
              .warn(
                Map("limit" -> limit.toString, "offset" -> offset.toString)
              )("Invalid pagination parameters")
              .as(Left(InvalidPagination))
          } else {
            store.list(limit, offset).flatMap { case (entities, total) =>
              logger
                .info(
                  Map(
                    "limit" -> limit.toString,
                    "offset" -> offset.toString,
                    "count" -> entities.size.toString,
                    "total" -> total.toString
                  )
                )("Request completed")
                .as(
                  Right(
                    (
                      entities.map(FulfilmentResponse.apply),
                      total.toString
                    )
                  )
                )
            }
          }
      } yield result
    }

  def routes[F[_]: Async](
      store: FulfilmentStore[F],
      logger: StructuredLogger[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      List(
        serverEndpoint(store, logger),
        getFulfilmentServerEndpoint(store, logger),
        updateFulfilmentServerEndpoint(store, logger),
        replaceFulfilmentServerEndpoint(store, logger),
        deleteFulfilmentServerEndpoint(store, logger),
        listFulfilmentsServerEndpoint(store, logger)
      )
    )
}
