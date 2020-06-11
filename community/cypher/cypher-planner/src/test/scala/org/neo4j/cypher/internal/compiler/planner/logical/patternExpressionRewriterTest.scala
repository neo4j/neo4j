/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.rewriting.rewriters.PatternExpressionPatternElementNamer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class patternExpressionRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("Rewrites pattern expressions") {
    // given
    val expr = and(patExpr1, patExpr2)
    val strategy = createStrategy
    val context = newMockedLogicalPlanningContext(newMockedPlanContext(), strategy = strategy)
    val rewriter = patternExpressionRewriter(Set.empty, context)

    // when
    expr.endoRewrite(rewriter)

    // then
    verify(strategy).planPatternExpression(Set.empty, patExpr1, context)
    verify(strategy).planPatternExpression(Set.empty, patExpr2, context)
    verifyNoMoreInteractions( strategy )
  }

  test("Does not rewrite pattern expressions on nested plans") {
    // given
    val expr = or(and(patExpr1, NestedPlanExpression.collect(dummyPlan, patExpr2, patExpr2)_), patExpr3)
    val strategy = createStrategy
    val context = newMockedLogicalPlanningContext(newMockedPlanContext(), strategy = strategy)
    val rewriter = patternExpressionRewriter(Set.empty, context)

    // when
    expr.endoRewrite(rewriter)

    // then
    verify(strategy).planPatternExpression(Set.empty, patExpr1, context)
    verify(strategy).planPatternExpression(Set.empty, patExpr3, context)
    verifyNoMoreInteractions( strategy )
  }

  test("Does rewrite pattern expressions inside nested plans") {
    // given
    val plan = Selection(Seq(patExpr3), dummyPlan)
    val expr = or(and(patExpr1, NestedPlanExpression.collect(plan, patExpr2, patExpr2)_), patExpr4)
    val strategy = createStrategy
    val context = newMockedLogicalPlanningContext(newMockedPlanContext(), strategy = strategy)
    val rewriter = patternExpressionRewriter(Set.empty, context)

    // when
    expr.endoRewrite(rewriter)

    // then
    verify(strategy).planPatternExpression(Set.empty, patExpr1, context)
    verify(strategy).planPatternExpression(Set.empty, patExpr3, context)
    verify(strategy).planPatternExpression(Set.empty, patExpr4, context)
    verifyNoMoreInteractions( strategy )
  }

  test("Does specialize Exists(PatternExpression)") {
    // given
    val expr: Expression = exists(patExpr1)
    val strategy = createStrategy
    val context = newMockedLogicalPlanningContext(newMockedPlanContext(), strategy = strategy)
    val rewriter = patternExpressionRewriter(Set.empty, context)

    // when
    val rewritten = expr.endoRewrite(rewriter)

    // then
    verify(strategy).planPatternExpression(Set.empty, patExpr1, context)
    verifyNoMoreInteractions( strategy )

    rewritten shouldBe a[NestedPlanExistsExpression]
  }

  private val patExpr1 = newPatExpr( "a", "b" )
  private val patExpr2 = newPatExpr( "c", "d" )
  private val patExpr3 = newPatExpr( "e", "f ")
  private val patExpr4 = newPatExpr( "g", "h" )

  private val dummyPlan = AllNodesScan("a", Set.empty)

  private def newPatExpr(left: String, right: String): PatternExpression = {
    PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor(left)), Seq.empty, None) _,
      RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING) _,
      NodePattern(Some(varFor(right)), Seq.empty, None) _) _) _)
  }

  private def createStrategy: QueryGraphSolver = {
    val strategy = mock[QueryGraphSolver]
    when(strategy.planPatternExpression(any[Set[String]], any[PatternExpression], any[LogicalPlanningContext])).thenAnswer(
      (invocation: InvocationOnMock) => {
        val expr: PatternExpression = invocation.getArgument(1)
        val (namedExpr, _) = PatternExpressionPatternElementNamer(expr)
        (dummyPlan, namedExpr)
      })
    strategy
  }
}
