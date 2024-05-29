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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.frontend.helpers.TestContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class TransformerTest extends CypherFunSuite {

  private case class TestPhase(
    override val postConditions: Set[StepSequencer.Condition],
    transformation: Any => Any = identity,
    override val invalidatedConditions: Set[StepSequencer.Condition] = Set.empty
  ) extends Phase[BaseContext, Any, Any] {
    override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING
    override def process(from: Any, context: BaseContext): Any = transformation(from)
  }

  private case class ExplodesWhen(condition: Any => Boolean = _ => false) extends ValidatingCondition {
    override def name: String = "Explodes"

    override def apply(in: Any)(cancellationChecker: CancellationChecker): Seq[String] =
      if (condition(in)) Seq(s"$in was not ok.") else Seq.empty
  }

  private val dummyPhase = TestPhase(Set.empty)

  test("should check post-conditions of single phase") {
    val phase = TestPhase(Set(ExplodesWhen(_ => true)))
    val exception = the[IllegalStateException] thrownBy phase.transform(42, TestContext())
    exception.getMessage should include("42 was not ok.")
  }

  test("should check post-conditions of first phase in pipeline") {
    val phase = TestPhase(Set(ExplodesWhen(_ => true)))
    val pipeLine = phase andThen dummyPhase
    val exception = the[IllegalStateException] thrownBy pipeLine.transform(42, TestContext())
    exception.getMessage should include("42 was not ok.")
  }

  test("should check post-conditions of second phase in pipeline") {
    val phase = TestPhase(Set(ExplodesWhen(_ => true)))
    val pipeLine = dummyPhase andThen phase
    val exception = the[IllegalStateException] thrownBy pipeLine.transform(42, TestContext())
    exception.getMessage should include("42 was not ok.")
  }

  test("should check post-conditions in between phases") {
    val phase1 = TestPhase(Set(ExplodesWhen(_ == 1)), _ => 1)
    val phase2 = TestPhase(Set.empty, _ => 2)
    val pipeLine = phase1 andThen phase2
    val exception = the[IllegalStateException] thrownBy pipeLine.transform(0, TestContext())
    exception.getMessage should include("1 was not ok.")
  }

  test("should check accumulated post-conditions on pipeline of length 2") {
    val phase1 = TestPhase(Set(ExplodesWhen(_ == 1)))
    val phase2 = TestPhase(Set.empty, _ => 1)
    val pipeLine = phase1 andThen phase2
    val exception = the[IllegalStateException] thrownBy pipeLine.transform(42, TestContext())
    exception.getMessage should include("1 was not ok.")
  }

  test("should check accumulated post-conditions from first phase on pipeline of length 3") {
    val phase1 = TestPhase(Set(ExplodesWhen(_ == 1)))
    val phase2 = TestPhase(Set.empty)
    val phase3 = TestPhase(Set.empty, _ => 1)
    val pipeLine = phase1 andThen phase2 andThen phase3
    val exception = the[IllegalStateException] thrownBy pipeLine.transform(0, TestContext())
    exception.getMessage should include("1 was not ok.")
  }

  test("should check accumulated post-conditions from second phase on pipeline of length 3") {
    val phase1 = TestPhase(Set.empty)
    val phase2 = TestPhase(Set(ExplodesWhen(_ == 1)))
    val phase3 = TestPhase(Set.empty, _ => 1)
    val pipeLine = phase1 andThen phase2 andThen phase3
    val exception = the[IllegalStateException] thrownBy pipeLine.transform(0, TestContext())
    exception.getMessage should include("1 was not ok.")
  }

  test("should not check a postCondition after it gets invalidated in-between") {
    val cond = ExplodesWhen(_ == 1)
    val phase1 = TestPhase(Set(cond))
    val phase2 = TestPhase(Set.empty, invalidatedConditions = Set(cond))
    val phase3 = TestPhase(Set.empty, _ => 1)
    val pipeLine = phase1 andThen phase2 andThen phase3
    noException should be thrownBy pipeLine.transform(0, TestContext())
  }

  test("should not check a postCondition after it gets invalidated at the end") {
    val cond = ExplodesWhen(_ == 1)
    val phase1 = TestPhase(Set(cond))
    val phase2 = TestPhase(Set.empty)
    val phase3 = TestPhase(Set.empty, _ => 1, invalidatedConditions = Set(cond))
    val pipeLine = phase1 andThen phase2 andThen phase3
    noException should be thrownBy pipeLine.transform(0, TestContext())
  }

  test("should check accumulated post-conditions that gets invalidated and then re-enabled") {
    val cond = ExplodesWhen(_ == 1)
    val phase1 = TestPhase(Set(cond))
    val phase2 = TestPhase(Set.empty, invalidatedConditions = Set(cond))
    val phase3 = TestPhase(Set(cond), _ => 1)
    val pipeLine = phase1 andThen phase2 andThen phase3
    val exception = the[IllegalStateException] thrownBy pipeLine.transform(0, TestContext())
    exception.getMessage should include("1 was not ok.")
  }

  test("postConditions and invalidatedConditions make sense") {
    val cond1 = ExplodesWhen(_ == 1)
    val cond2 = ExplodesWhen(_ == 2)
    val cond3 = ExplodesWhen(_ == 3)

    def p(y: Set[StepSequencer.Condition], n: Set[StepSequencer.Condition] = Set.empty): TestPhase =
      TestPhase(y, invalidatedConditions = n)

    If((_: Any) => true)(p(Set(cond1))).postConditions shouldBe Set()
    If((_: Any) => true)(p(Set.empty, Set(cond1))).invalidatedConditions shouldBe Set(cond1)

    (p(Set(cond1)) andThen p(Set(cond2))).postConditions shouldBe Set(cond1, cond2)
    (p(Set(cond1)) andThen p(Set(cond2), Set(cond1))).postConditions shouldBe Set(cond2)
    (p(Set(cond1), Set(cond3)) andThen p(Set(cond2))).postConditions shouldBe Set(cond1, cond2)

    (p(Set(cond1)) andThen p(Set(cond2))).invalidatedConditions shouldBe Set()
    (p(Set(cond1)) andThen p(Set(cond2), Set(cond1))).invalidatedConditions shouldBe Set(cond1)
    (p(Set(cond1), Set(cond3)) andThen p(Set(cond2), Set(cond1))).invalidatedConditions shouldBe Set(cond1, cond3)
    (p(Set(cond1), Set(cond3)) andThen p(Set(cond3), Set(cond1))).invalidatedConditions shouldBe Set(cond1)
  }
}
