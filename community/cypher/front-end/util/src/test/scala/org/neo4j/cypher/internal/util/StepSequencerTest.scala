/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.StepSequencer.StepAccumulator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.Set

//noinspection ZeroIndexToHead
class StepSequencerTest extends CypherFunSuite {

  private case object condA extends Condition
  private case object condB extends Condition
  private case object condC extends Condition
  private case object condD extends Condition
  private case object condE extends Condition
  private case object condF extends Condition
  private case object condG extends Condition

  // No case class since we want reference equality
  class TestStep(name: String,
                 override val preConditions: Set[Condition],
                 override val postConditions: Set[Condition],
                 override val invalidatedConditions: Set[Condition]) extends Step {
    override def toString: String = s"TestStep($name)"
  }

  private val stepAccumulator = new StepAccumulator[Step, Seq[Step]] {
    override def empty: Seq[Step] = Seq.empty
    override def addNext(acc: Seq[Step], step: Step): Seq[Step] = acc :+ step
  }

  private val sequencer = StepSequencer(stepAccumulator)

  test("fails if a step depends on itself") {
    val steps = Seq(
      new TestStep("0", Set(condA), Set(condA), Set()),
    )
    an[IllegalArgumentException] should be thrownBy sequencer.orderSteps(steps.toSet)
  }

  test("fails if there are circular dependencies") {
    val steps = Seq(
      new TestStep("0", Set(condC), Set(condA), Set()),
      new TestStep("1", Set(condA), Set(condB), Set()),
      new TestStep("2", Set(condB), Set(condC), Set()),
    )
    an[IllegalArgumentException] should be thrownBy sequencer.orderSteps(steps.toSet)
  }

  test("fails if a step does not have any post-conditions") {
    val steps = Seq(
      new TestStep("0", Set(), Set(), Set()),
    )
    an[IllegalArgumentException] should be thrownBy sequencer.orderSteps(steps.toSet)
  }

  test("fails if multiple steps have the same post-conditions") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(), Set(condA), Set()),
    )
    an[IllegalArgumentException] should be thrownBy sequencer.orderSteps(steps.toSet)
  }

  test("fails if a step undoes its own work") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set(condA)),
    )
    an[IllegalArgumentException] should be thrownBy sequencer.orderSteps(steps.toSet)
  }

  test("fails if there is circular invalidation") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set(condB)),
      new TestStep("1", Set(), Set(condB), Set(condC)),
      new TestStep("2", Set(), Set(condC), Set(condA)),
    )
    an[IllegalArgumentException] should be thrownBy sequencer.orderSteps(steps.toSet)
  }

  test("orders steps correctly if they have strong ordering requirements") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(condA), Set(condB), Set()),
      new TestStep("2", Set(condB), Set(condC), Set()),
    )
    sequencer.orderSteps(steps.toSet) should equal(steps)
  }

  test("if only some steps have dependencies, order those correctly") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(), Set(condB), Set()),
      new TestStep("2", Set(), Set(condC), Set()),
      new TestStep("3", Set(condA), Set(condD), Set()),
      new TestStep("4", Set(condA), Set(condE), Set()),
      new TestStep("5", Set(condB), Set(condF), Set()),
      new TestStep("6", Set(condB, condA), Set(condG), Set()),
    )

    val orderedSteps = sequencer.orderSteps(steps.toSet)
    withClue(orderedSteps) {
      orderedSteps.indexOfOrFail(steps(3)) should be > orderedSteps.indexOfOrFail(steps(0))
      orderedSteps.indexOfOrFail(steps(4)) should be > orderedSteps.indexOfOrFail(steps(0))
      orderedSteps.indexOfOrFail(steps(5)) should be > orderedSteps.indexOfOrFail(steps(1))
      orderedSteps.indexOfOrFail(steps(6)) should be > orderedSteps.indexOfOrFail(steps(0))
      orderedSteps.indexOfOrFail(steps(6)) should be > orderedSteps.indexOfOrFail(steps(1))
    }
  }

  test("duplicates single step if needed because conditions get invalidated") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(condA), Set(condB), Set()),
      new TestStep("2", Set(condB), Set(condC), Set(condB)),
    )
    sequencer.orderSteps(steps.toSet) should equal(Seq(
      steps(0),
      steps(1),
      steps(2),
      steps(1),
    ))
  }

  test("orders independent steps correctly if conditions get invalidated") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(condA), Set(condB), Set(condA)),
      new TestStep("2", Set(condA), Set(condC), Set()),
    )
    sequencer.orderSteps(steps.toSet) should (equal(Seq(
      steps(0),
      steps(2),
      steps(1),
      steps(0),
    )) or equal(Seq(
      steps(0),
      steps(1),
      steps(0),
      steps(2),
    )))
  }

  test("duplicates multiple steps if needed because conditions get invalidated") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(condA), Set(condB), Set()),
      new TestStep("2", Set(condB), Set(condC), Set(condA, condB)),
    )
    sequencer.orderSteps(steps.toSet) should equal(Seq(
      steps(0),
      steps(1),
      steps(2),
      steps(0),
      steps(1),
    ))
  }

  test("duplicates steps multiple times if needed because conditions get invalidated") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(condA), Set(condB), Set(condA)),
      new TestStep("2", Set(condA), Set(condC), Set(condA)),
    )
    sequencer.orderSteps(steps.toSet) should (equal(Seq(
      steps(0),
      steps(1),
      steps(0),
      steps(2),
      steps(0),
    )) or equal(Seq(
      steps(0),
      steps(2),
      steps(0),
      steps(1),
      steps(0),
    )))
  }

  test("very invalidating steps") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(condA), Set(condB), Set(condA)),
      new TestStep("2", Set(condA, condB), Set(condC), Set(condA, condB)),
    )
    sequencer.orderSteps(steps.toSet) should equal(Seq(
      steps(0),
      steps(1),
      steps(0),
      steps(2),
      steps(0),
      steps(1),
      steps(0),
    ))
  }

  test("should order invalidating step first if it has no dependencies") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(), Set(condB), Set()),
      new TestStep("2", Set(), Set(condE), Set(condA, condB, condC, condD)),
      new TestStep("3", Set(), Set(condC), Set()),
      new TestStep("4", Set(), Set(condD), Set()),
      new TestStep("5", Set(condA, condB, condC, condD), Set(condF), Set()),
    )

    val orderedSteps = sequencer.orderSteps(steps.toSet)
    withClue(orderedSteps) {
      orderedSteps.size should equal(steps.size)
      orderedSteps(0) should equal(steps(2))
      orderedSteps(5) should equal(steps(5))
    }
  }

  test("should order often invalidated step last if it has no dependencies") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set(condE)),
      new TestStep("1", Set(), Set(condB), Set(condE)),
      new TestStep("2", Set(), Set(condE), Set(condF, condG)),
      new TestStep("3", Set(), Set(condC), Set(condE)),
      new TestStep("4", Set(), Set(condD), Set(condE)),
      new TestStep("5", Set(condA, condB, condC, condD, condE), Set(condF, condG), Set()),
    )

    val orderedSteps = sequencer.orderSteps(steps.toSet)
    withClue(orderedSteps) {
      orderedSteps.size should equal(steps.size)
      orderedSteps(4) should equal(steps(2))
      orderedSteps(5) should equal(steps(5))
    }
  }

  test("should order often invalidated step last if it has no dependencies 2") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA, condB, condC), Set()),
      new TestStep("1", Set(), Set(condD), Set(condA)),
      new TestStep("2", Set(), Set(condE), Set(condA)),
      new TestStep("3", Set(), Set(condF), Set(condA, condB)),
    )

    val orderedSteps = sequencer.orderSteps(steps.toSet)
    withClue(orderedSteps) {
      orderedSteps.size should equal(steps.size)
      orderedSteps(3) should equal(steps(0))
    }
  }

  test("should find order where step that introduces frequently invalidated condition is not repeated too often") {
    val steps = Seq(
      new TestStep("0", Set(), Set(condA), Set()),
      new TestStep("1", Set(condA), Set(condB), Set()),
      new TestStep("2", Set(condA), Set(condC), Set(condB)),
      new TestStep("3", Set(condA), Set(condD), Set(condB)),
      new TestStep("4", Set(condB, condC, condD), Set(condE), Set()),
    )
    sequencer.orderSteps(steps.toSet) should (equal(Seq(
      steps(0),
      steps(2),
      steps(3),
      steps(1),
      steps(4),
    )) or equal(Seq(
      steps(0),
      steps(3),
      steps(2),
      steps(1),
      steps(4),
    )))
  }

  implicit class Indexer[S](s: Seq[S]) {
    def indexOfOrFail(elem: S): Int = {
      val i = s.indexOf(elem)
      if (i == -1) throw new IllegalArgumentException(s"$elem not in $s")
      i
    }
  }
}
