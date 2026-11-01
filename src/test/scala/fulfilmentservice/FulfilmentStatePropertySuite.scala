package fulfilmentservice

import fulfilmentservice.domain.{FulfilmentState}

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class FulfilmentStatePropertySuite extends ScalaCheckSuite {

  property("every state's wire value parses back to itself") {
    forAll(Gen.oneOf(FulfilmentState.values.toList)) { state =>
      assertEquals(FulfilmentState.fromString(state.wire), Some(state))
    }
  }
}
