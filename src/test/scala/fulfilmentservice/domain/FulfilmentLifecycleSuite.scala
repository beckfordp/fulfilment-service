package fulfilmentservice.domain

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class FulfilmentLifecycleSuite extends ScalaCheckSuite {

  import FulfilmentCommand.{Cancel, Pack, StartPicking}
  import FulfilmentState._

  private def dispatch: FulfilmentCommand.Dispatch =
    FulfilmentCommand.dispatch("GLN-1", "DHL") match {
      case Right(command) => command
      case Left(error)    => fail(s"unexpected $error")
    }

  private def failCommand: FulfilmentCommand.Fail =
    FulfilmentCommand.fail("damaged") match {
      case Right(command) => command
      case Left(error)    => fail(s"unexpected $error")
    }

  private val commands: Gen[FulfilmentCommand] =
    Gen.oneOf(List(StartPicking, Pack, Cancel, dispatch, failCommand))

  private val states: Gen[FulfilmentState] =
    Gen.oneOf(FulfilmentState.values.toList)

  test("legal transitions follow PENDING -> PICKING -> PACKED -> DISPATCHED") {
    assertEquals(
      FulfilmentLifecycle.transition(Pending, StartPicking),
      Right(Picking)
    )
    assertEquals(FulfilmentLifecycle.transition(Picking, Pack), Right(Packed))
    assertEquals(
      FulfilmentLifecycle.transition(Packed, dispatch),
      Right(Dispatched)
    )
  }

  test("fail is legal from every non-terminal state") {
    List(Pending, Picking, Packed).foreach { state =>
      assertEquals(
        FulfilmentLifecycle.transition(state, failCommand),
        Right(DispatchFailed)
      )
    }
  }

  test("cancel is legal from every non-terminal state") {
    List(Pending, Picking, Packed).foreach { state =>
      assertEquals(
        FulfilmentLifecycle.transition(state, Cancel),
        Right(Cancelled)
      )
    }
  }

  test("out-of-order steps are rejected with the source state") {
    assertEquals(
      FulfilmentLifecycle.transition(Pending, Pack),
      Left(IllegalTransition(Pending))
    )
    assertEquals(
      FulfilmentLifecycle.transition(Picking, dispatch),
      Left(IllegalTransition(Picking))
    )
    assertEquals(
      FulfilmentLifecycle.transition(Packed, StartPicking),
      Left(IllegalTransition(Packed))
    )
  }

  property("no command leaves a terminal state") {
    forAll(Gen.oneOf(Dispatched, DispatchFailed, Cancelled), commands) {
      (state, command) =>
        assertEquals(
          FulfilmentLifecycle.transition(state, command),
          Left(IllegalTransition(state))
        )
    }
  }

  property("a legal transition never targets PENDING") {
    forAll(states, commands) { (state, command) =>
      assert(!FulfilmentLifecycle.transition(state, command).contains(Pending))
    }
  }

  test("dispatch trims and requires trackingNo and carrier") {
    assertEquals(
      FulfilmentCommand
        .dispatch("  GLN-1 ", " DHL ")
        .map(d => (d.trackingNo, d.carrier)),
      Right(("GLN-1", "DHL"))
    )
    List(("", "DHL"), ("GLN-1", "  ")).foreach { case (tracking, carrier) =>
      assertEquals(
        FulfilmentCommand.dispatch(tracking, carrier),
        Left(
          FulfilmentFailure.InvalidRequest(
            "trackingNo and carrier are required"
          )
        )
      )
    }
  }

  test("fail trims and requires a reason") {
    assertEquals(
      FulfilmentCommand.fail(" damaged ").map(_.reason),
      Right("damaged")
    )
    assertEquals(
      FulfilmentCommand.fail("   "),
      Left(FulfilmentFailure.InvalidRequest("reason is required"))
    )
  }

  test("simulatedNext advances one step and stops at terminal states") {
    assertEquals(
      FulfilmentLifecycle.simulatedNext(Pending, dispatch),
      Some(StartPicking)
    )
    assertEquals(
      FulfilmentLifecycle.simulatedNext(Picking, dispatch),
      Some(Pack)
    )
    assertEquals(
      FulfilmentLifecycle.simulatedNext(Packed, dispatch),
      Some(dispatch)
    )
    assertEquals(FulfilmentLifecycle.simulatedNext(Dispatched, dispatch), None)
    assertEquals(
      FulfilmentLifecycle.simulatedNext(DispatchFailed, dispatch),
      None
    )
    assertEquals(FulfilmentLifecycle.simulatedNext(Cancelled, dispatch), None)
  }

  property("every simulated step is a legal transition") {
    forAll(states) { state =>
      FulfilmentLifecycle.simulatedNext(state, dispatch).forall { command =>
        FulfilmentLifecycle.transition(state, command).isRight
      }
    }
  }
}
