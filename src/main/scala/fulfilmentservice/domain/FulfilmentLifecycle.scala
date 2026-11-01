package fulfilmentservice.domain

/** A fulfilment task's lifecycle status. `wire` is the value stored in Postgres
  * and returned by the operator API.
  */
enum FulfilmentState(val wire: String) {
  case Pending extends FulfilmentState("PENDING")
  case Picking extends FulfilmentState("PICKING")
  case Packed extends FulfilmentState("PACKED")
  case Dispatched extends FulfilmentState("DISPATCHED")
  case DispatchFailed extends FulfilmentState("DISPATCH_FAILED")
}

object FulfilmentState {
  def fromString(value: String): Option[FulfilmentState] =
    values.find(_.wire == value)
}

/** Expected failures of the operator API, mapped to HTTP statuses at the edge.
  */
sealed trait FulfilmentFailure

object FulfilmentFailure {
  case object NotFound extends FulfilmentFailure
  final case class InvalidTransition(message: String) extends FulfilmentFailure
  final case class InvalidRequest(message: String) extends FulfilmentFailure
}

/** An operator (or simulated) step on a task. `Dispatch`/`Fail` are only built
  * via the smart constructors, so their fields are always trimmed and non-blank
  * (parse, don't validate - scala-fp.md law 5).
  */
sealed trait FulfilmentCommand

object FulfilmentCommand {
  case object StartPicking extends FulfilmentCommand
  case object Pack extends FulfilmentCommand
  final case class Dispatch private[domain] (
      trackingNo: String,
      carrier: String
  ) extends FulfilmentCommand
  final case class Fail private[domain] (reason: String)
      extends FulfilmentCommand

  def dispatch(
      trackingNo: String,
      carrier: String
  ): Either[FulfilmentFailure.InvalidRequest, Dispatch] = {
    val tracking = trackingNo.trim
    val carrierName = carrier.trim
    if (tracking.isEmpty || carrierName.isEmpty)
      Left(
        FulfilmentFailure.InvalidRequest("trackingNo and carrier are required")
      )
    else Right(new Dispatch(tracking, carrierName))
  }

  def fail(reason: String): Either[FulfilmentFailure.InvalidRequest, Fail] = {
    val trimmed = reason.trim
    if (trimmed.isEmpty)
      Left(FulfilmentFailure.InvalidRequest("reason is required"))
    else Right(new Fail(trimmed))
  }
}

/** The command isn't legal from `from`. */
final case class IllegalTransition(from: FulfilmentState)

/** The fulfilment state machine, defined once (scala-fp.md laws 1-2): every
  * status change - operator API, simulate-next - goes through `transition`.
  */
object FulfilmentLifecycle {
  import FulfilmentCommand._
  import FulfilmentState._

  def transition(
      current: FulfilmentState,
      command: FulfilmentCommand
  ): Either[IllegalTransition, FulfilmentState] =
    (current, command) match {
      case (Pending, StartPicking)               => Right(Picking)
      case (Picking, Pack)                       => Right(Packed)
      case (Packed, Dispatch(_, _))              => Right(Dispatched)
      case (Pending | Picking | Packed, Fail(_)) => Right(DispatchFailed)
      case (Pending, Pack | Dispatch(_, _)) => Left(IllegalTransition(current))
      case (Picking, StartPicking | Dispatch(_, _)) =>
        Left(IllegalTransition(current))
      case (Packed, StartPicking | Pack)    => Left(IllegalTransition(current))
      case (Dispatched | DispatchFailed, _) => Left(IllegalTransition(current))
    }

  /** simulate-next's one step forward; `None` once terminal. */
  def simulatedNext(
      current: FulfilmentState,
      dispatch: Dispatch
  ): Option[FulfilmentCommand] =
    current match {
      case Pending                     => Some(StartPicking)
      case Picking                     => Some(Pack)
      case Packed                      => Some(dispatch)
      case Dispatched | DispatchFailed => None
    }
}
