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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.calls
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.patternExpression
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.varFor
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.distributeLawsRewriter.dnfCounts
import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.PredicateTestSupport
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.annotation.tailrec

class DistributeLawRewriterTest extends CypherFunSuite with PredicateTestSupport {

  override def rewriter: Rewriter = getRewriterAndMonitor._1

  private def getRewriterAndMonitor: (Rewriter, AstRewritingMonitor) = {
    val monitor: AstRewritingMonitor = mock[AstRewritingMonitor]
    val rewriter: Rewriter = distributeLawsRewriter(CancellationChecker.neverCancelled())(monitor)
    (rewriter, monitor)
  }

  test("(P or (Q and R))  iff  (P or Q) and (P or R)") {
    or(P, and(Q, R)) <=> and(or(P, Q), or(P, R))
  }

  test("((Q and R) or P)  iff  (Q or P) and (R or P)") {
    or(and(Q, R), P) <=> and(or(Q, P), or(R, P))
  }

  test("((Q and R and S) or P)  iff  (Q or P) and (R or P) and (S or P)") {
    or(and(Q, and(R, S)), P) <=> and(or(Q, P), and(or(R, P), or(S, P)))
  }

  test("nested or and nested and") {
    or(or(P, Q), and(R, S)) <=> and(or(or(P, Q), R), or(or(P, Q), S))
  }

  test("nested or and nested and 2") {
    or(or(P, and(Q, R)), and(S, T)) <=>
      and(
        and(or(or(P, Q), S), or(or(P, R), S)),
        and(or(or(P, Q), T), or(or(P, R), T))
      )
  }

  test("should not attempt to rewrite DNF predicates larger than the DNF_CONVERSION_LIMIT") {
    // given
    val start = or(and(P, Q), and(Q, R))
    val fullOr = combineUntilLimit(start, distributeLawsRewriter.DNF_CONVERSION_LIMIT - 1)
    val (rewriter, monitor) = getRewriterAndMonitor

    // when
    val result = rewriter.apply(fullOr)

    // then result should be the same and aborted due to DNF-limit
    result should be(fullOr)
    verify(monitor).abortedRewritingDueToLargeDNF(fullOr)
  }

  test("should attempt to rewrite DNF predicates smaller than the DNF_CONVERSION_LIMIT") {
    // given
    val start = or(and(P, Q), and(Q, R))
    val fullOr = combineUntilLimit(start, distributeLawsRewriter.DNF_CONVERSION_LIMIT - 2)
    val (rewriter, monitor) = getRewriterAndMonitor

    // when
    val result = rewriter.apply(fullOr)

    // When attempting to convert the expression, we still hit the size limit in `repeatWithSizeLimit` and will abort
    result should be(fullOr)
    verify(monitor).abortedRewriting(fullOr)
  }

  test("should succeed to rewrite DNF predicates much smaller than the DNF_CONVERSION_LIMIT") {
    // given
    val start = or(and(P, Q), and(Q, R))
    val fullOr = combineUntilLimit(start, 2)
    val (rewriter, monitor) = getRewriterAndMonitor

    // when
    val result = rewriter.apply(fullOr)

    result should not be fullOr
    verifyNoInteractions(monitor)
  }

  test(
    "should rewrite 2 disjoint DNF predicates smaller than the limit, even if together they would be bigger than the limit"
  ) {
    // given
    val start1 = or(and(P, Q), and(Q, R))
    val fullOr1 = combineUntilLimit(start1, distributeLawsRewriter.DNF_CONVERSION_LIMIT - 2)
    val start2 = or(and(S, Q), and(Q, R))
    val fullOr2 = combineUntilLimit(start2, distributeLawsRewriter.DNF_CONVERSION_LIMIT - 2)
    val fullExp = and(fullOr1, fullOr2)
    val (rewriter, monitor) = getRewriterAndMonitor

    // when
    val result = rewriter.apply(fullExp)

    // When attempting to convert the expressions, we still hit the size limit in `repeatWithSizeLimit` and will abort
    result should be(fullExp)
    val inOrder = Mockito.inOrder(monitor)
    inOrder.verify(monitor, calls(1)).abortedRewriting(fullOr1)
    inOrder.verify(monitor, calls(1)).abortedRewriting(fullOr2)
    inOrder.verifyNoMoreInteractions()
  }

  test(
    "in a large AST, should apply size limit of applicable OR (not size limit of whole AST)"
  ) {
    // given
    val start = or(and(P, Q), and(Q, R))
    val fullOr = combineUntilLimit(start, 4) // 4 is the smallest number that leads to an abort
    // Create an expression that is (empirically determined to be) larger than the fully rewritten fullOr
    val largeOtherExpression = nots(P, 1000)
    val fullExp = and(fullOr, largeOtherExpression)
    val (rewriter, monitor) = getRewriterAndMonitor

    // when
    rewriter.apply(fullExp)

    // then
    verify(monitor).abortedRewriting(any[AnyRef])
  }

  test("should rewrite small expression containing pattern expressions") {
    val pat = patternExpression(varFor("a"), varFor("b"))
    val exp = combineUntilLimit(and(P, pat), limit = 2)
    val (rewriter, monitor) = getRewriterAndMonitor

    rewriter.apply(exp)
    verify(monitor, never()).abortedRewritingDueToLargeDNF(exp)
  }

  test("should abort rewriting larger expression containing pattern expressions") {
    val pat = patternExpression(varFor("a"), varFor("b"))
    val exp = combineUntilLimit(and(P, pat), limit = 4)
    val (rewriter, monitor) = getRewriterAndMonitor

    rewriter.apply(exp)
    verify(monitor).abortedRewritingDueToLargeDNF(exp)
  }

  test(
    "should rewrite DNF predicates smaller than the limit, larger than half the limit, even if pattern expression is present elsewhere"
  ) {
    // given
    val start = or(and(P, Q), and(Q, R))
    val fullOr = combineUntilLimit(start, distributeLawsRewriter.DNF_CONVERSION_LIMIT - 2)
    val fullExp = and(fullOr, patternExpression(varFor("a"), varFor("b")))
    val (rewriter, monitor) = getRewriterAndMonitor

    // when
    val result = rewriter.apply(fullExp)

    // When attempting to convert the expression, we still hit the size limit in `repeatWithSizeLimit` and will abort
    result should be(fullExp)
    verify(monitor).abortedRewriting(fullOr)
  }

  // Tests for dnfCounts

  test("should not count non-applicable or") {
    dnfCounts(or(P, or(Q, R))) should be(0)
  }

  test("should count applicable or - RHS") {
    dnfCounts(or(P, and(Q, R))) should be(1)
  }

  test("should count applicable or - LHS") {
    dnfCounts(or(and(Q, R), P)) should be(1)
  }

  test("should double count applicable or with nested double and") {
    dnfCounts(or(P, and(Q, and(R, S)))) should be(2)
  }

  test("should triple count applicable or with nested triple and") {
    dnfCounts(or(P, and(Q, and(and(S, T), S)))) should be(3)
  }

  test("should not double count applicable or with nested ands bot with not in between") {
    dnfCounts(or(P, and(Q, not(and(R, S))))) should be(1)
  }

  test("should double count doubly-applicable or") {
    dnfCounts(or(and(Q, R), and(Q, R))) should be(2)
  }

  test("should count nested applicable or") {
    dnfCounts(or(P, or(P, and(Q, R)))) should be(1)
  }

  test("should count two nested applicable ors") {
    dnfCounts(or(or(P, and(Q, R)), or(P, and(Q, R)))) should be(2)
  }

  @tailrec
  private def combineUntilLimit(start: Expression, limit: Int): Expression =
    if (limit > 0)
      combineUntilLimit(or(start, and(P, Q)), limit - 1)
    else
      start

  private def nots(start: Expression, limit: Int): Expression =
    if (limit > 0)
      nots(not(start), limit - 1)
    else
      start
}
