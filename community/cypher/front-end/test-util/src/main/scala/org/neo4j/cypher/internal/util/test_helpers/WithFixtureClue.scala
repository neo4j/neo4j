/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.test_helpers

import org.scalatest.Failed
import org.scalatest.Outcome
import org.scalatest.TestSuite
import org.scalatest.TestSuiteMixin
import org.scalatest.exceptions.ModifiableMessage

/**
 * A [[TestSuiteMixin]] that includes [[testFailureClue]] in every test failure, also when the failure is reported
 * through an exception that cannot carry a clue (then printed to stderr instead).
 *
 * Why this exists: a Scala 3 test trait that overrides `withFixture` and calls `super.withFixture` generates a
 * super-accessor the Scala 2.13 TASTy reader cannot reconstruct, making every Scala 2.13 suite mixing it fail to
 * compile ("class needs to be abstract"). This trait is compiled by Scala 2.13 (so its `abstract override` +
 * `super` is fine); Scala 3 traits extend it and only override [[testFailureClue]] (a plain override, no
 * super-call), keeping them readable from Scala 2.13.
 */
trait WithFixtureClue extends TestSuiteMixin with TestSuite {

  /**
   * The clue to include in test failures. Evaluated before each test runs; defer any computation that is only
   * available while the test runs to the returned object's `toString`.
   */
  protected def testFailureClue: AnyRef

  abstract override def withFixture(test: NoArgTest): Outcome = {
    val clue = testFailureClue
    withClue(clue) {
      try {
        val outcome = super.withFixture(test)
        outcome match {
          case Failed(_: ModifiableMessage[_]) =>
          // Clue will be included in the exception by the wrapping withClue
          case Failed(_) =>
            // Print clue to stderr since withClue won't include it
            System.err.println(clue)
          case _ =>
          // Do nothing
        }
        outcome
      } catch {
        case e: Throwable if !e.isInstanceOf[ModifiableMessage[_]] =>
          // Print clue to stderr
          System.err.println(clue)
          throw e
      }
    }
  }
}
