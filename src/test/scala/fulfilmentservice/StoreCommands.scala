package fulfilmentservice

import cats.Applicative
import fulfilmentservice.domain.{FulfilmentCommand, FulfilmentFailure, OrderId}

/** Test-only shorthands over `FulfilmentStore.transition`, parsing raw
  * dispatch/fail fields the way the routes do.
  */
object StoreCommands {
  extension [F[_]: Applicative](store: FulfilmentStore[F]) {
    def startPicking(
        orderId: OrderId
    ): F[Either[FulfilmentFailure, FulfilmentTask]] =
      store.transition(orderId, FulfilmentCommand.StartPicking)

    def pack(orderId: OrderId): F[Either[FulfilmentFailure, FulfilmentTask]] =
      store.transition(orderId, FulfilmentCommand.Pack)

    def dispatch(
        orderId: OrderId,
        trackingNo: String,
        carrier: String
    ): F[Either[FulfilmentFailure, FulfilmentTask]] =
      FulfilmentCommand.dispatch(trackingNo, carrier) match {
        case Left(error)    => Applicative[F].pure(Left(error))
        case Right(command) => store.transition(orderId, command)
      }

    def cancel(
        orderId: OrderId
    ): F[Either[FulfilmentFailure, FulfilmentTask]] =
      store.transition(orderId, FulfilmentCommand.Cancel)

    def fail(
        orderId: OrderId,
        reason: String
    ): F[Either[FulfilmentFailure, FulfilmentTask]] =
      FulfilmentCommand.fail(reason) match {
        case Left(error)    => Applicative[F].pure(Left(error))
        case Right(command) => store.transition(orderId, command)
      }
  }
}
