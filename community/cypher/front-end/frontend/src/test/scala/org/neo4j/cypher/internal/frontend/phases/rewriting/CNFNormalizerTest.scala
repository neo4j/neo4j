/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.phases.rewriting

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CNFNormalizer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.PredicateTestSupport
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.mock.MockitoSugar

class CNFNormalizerTest extends CypherFunSuite with PredicateTestSupport {

  var rewriter: Rewriter = _
  var astRewritingMonitor: AstRewritingMonitor = _

  test("should not touch a simple predicate") {
    P <=> P
  }

  test("should lift plain AND") {
    and(P, Q) <=> ands(P, Q)
  }

  test("should flatten multiple ANDs in a ANDS") {
    and(P, and(R, S)) <=> ands(P, R, S)
    and(and(R, S), P) <=> ands(R, S, P)
  }

  test("should be able to convert a dnf to cnf") {
    or(and(P, Q), and(R, S)) <=> ands(ors(P, R), ors(Q, R), ors(P, S), ors(Q, S))
  }

  test("should be able to convert a dnf with nested ands to cnf") {
    or(and(P, and(Q, V)), and(R, S)) <=> ands(ors(P, R), ors(Q, R), ors(V, R), ors(P, S), ors(Q, S), ors(V, S))
  }

  test("should be able to convert a complex formula to cnf") {
    or(xor(P, Q), xor(R, S)) <=> ands(
      ors(P, Q, R, S),
      ors(not(P), not(Q), R, S),
      ors(P, Q, not(R), not(S)),
      ors(not(P), not(Q), not(R), not(S))
    )
  }

  test("aborts cnf-rewriting for the worst case scenarios") {
    /* GIVEN A PATHOLOGICAL CASE FOR CNF
    When we get predicates in certain shapes, the normalized form for the predicate is so large it becomes
    intractable to plan on, and the better alternative is to simply give up on normalizing and let the planner
    produces a sub-optimal plan instead.
     */

    val p1 = anExp("p1")
    val p2 = anExp("p2")
    val p3 = anExp("p3")
    val p4 = anExp("p4")
    val p5 = anExp("p5")
    val p6 = anExp("p6")
    val p7 = anExp("p7")
    val p8 = anExp("p8")
    val p9 = anExp("p9")
    val p10 = anExp("p10")
    val p11 = anExp("p11")
    val p12 = anExp("p12")
    val p13 = anExp("p13")
    val p14 = anExp("p14")
    val p15 = anExp("p15")
    val p16 = anExp("p16")
    val p17 = anExp("p17")
    val p18 = anExp("p18")
    val p19 = anExp("p19")
    val p20 = anExp("p20")

    val bigPredicate =
      or(
        or(
          or(
            or(
              and(p1, p2),
              and(p3, p4)), or(
              and(p5, p6),
              and(p7, p8))), or(
            or(
              and(p9, p10),
              and(p11, p12)), or(
              and(p13, p14),
              and(p15, p16)))), or(
          and(p17, p18),
          and(p19, p20)))

    // When
    bigPredicate.rewrite(rewriter)

    // Then the rewriting was aborted
    verify(astRewritingMonitor).abortedRewriting(any())
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val monitors = mock[Monitors]
    astRewritingMonitor = mock[AstRewritingMonitor]
    when(monitors.newMonitor[AstRewritingMonitor]()).thenReturn(astRewritingMonitor)
    rewriter = CNFNormalizer.instance(new TestContext(monitors))
  }
}

object TestContext extends MockitoSugar {
  def apply() = new TestContext(mock[Monitors])

  def apply(monitors: Monitors): TestContext = new TestContext(monitors)
}

class TestContext(override val monitors: Monitors) extends BaseContext {
  override def tracer: CompilationPhaseTracer = ???

  override def notificationLogger: InternalNotificationLogger = ???

  override def cypherExceptionFactory: CypherExceptionFactory = ???

  override def errorHandler: Seq[SemanticErrorDef] => Unit = ???
}
