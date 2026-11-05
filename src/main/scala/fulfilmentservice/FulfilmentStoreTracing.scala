package fulfilmentservice

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import fulfilmentservice.domain.{FulfilmentCommand, FulfilmentFailure, OrderId}
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.Tracer

/** Fulfilment business spans (gluon PERF-5.9), as a `FulfilmentStore` decorator
  * over the same steps `FulfilmentJourneyMetrics` wraps; each is one Postgres
  * transaction, so its span is that step's DB transaction too. Wired outermost,
  * so milestone log lines and the `outbox.write` span nest inside. A raised
  * error marks the span Error (otel4s's default finalization).
  *
  *   - `fulfilment.create_task{order.id}`
  *   - `fulfilment.transition{order.id, fulfilment.command}`: a
  *     `fulfilment.status_changed{from,to}` event when applied,
  *     `outcome=rejected` when not
  */
object FulfilmentStoreTracing {

  /** Low-cardinality command name: no tracking numbers or failure reasons. */
  def commandName(command: FulfilmentCommand): String = command match {
    case FulfilmentCommand.StartPicking => "StartPicking"
    case FulfilmentCommand.Pack         => "Pack"
    case FulfilmentCommand.Cancel       => "Cancel"
    case _: FulfilmentCommand.Dispatch  => "Dispatch"
    case _: FulfilmentCommand.Fail      => "Fail"
  }

  def instrument[F[_]: MonadCancelThrow](
      store: FulfilmentStore[F],
      tracer: Tracer[F]
  ): FulfilmentStore[F] =
    new FulfilmentStore[F] {
      export store.{createTask => _, transitionFrom => _, *}

      def createTask(
          orderId: OrderId,
          items: List[FulfilmentItem],
          shippingAddress: Address
      ): F[FulfilmentTask] =
        tracer
          .span(
            "fulfilment.create_task",
            Attribute("order.id", OrderId.render(orderId))
          )
          .surround(store.createTask(orderId, items, shippingAddress))

      def transitionFrom(
          orderId: OrderId,
          command: FulfilmentCommand
      ): F[Either[FulfilmentFailure, (FulfilmentTask, FulfilmentTask)]] =
        tracer
          .span(
            "fulfilment.transition",
            Attribute("order.id", OrderId.render(orderId)),
            Attribute("fulfilment.command", commandName(command))
          )
          .use { span =>
            store.transitionFrom(orderId, command).flatTap {
              case Right((prev, after)) =>
                span.addEvent(
                  "fulfilment.status_changed",
                  Attribute("from", prev.state.wire),
                  Attribute("to", after.state.wire)
                )
              case Left(_) =>
                span.addAttribute(Attribute("outcome", "rejected"))
            }
          }
    }
}
