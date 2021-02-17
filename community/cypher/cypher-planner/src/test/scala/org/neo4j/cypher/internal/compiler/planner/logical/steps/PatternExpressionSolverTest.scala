/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.MapKeys

class PatternExpressionSolverTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val patExpr1 = newPatExpr("a", 0, 1, 2, SemanticDirection.OUTGOING)
  private val patExpr2 = newPatExpr("a", 3, 4, 5, SemanticDirection.INCOMING)

  test("Rewrites single pattern expression") {
    // given MATCH (a) RETURN (a)-->() as x
    val strategy = mock[QueryGraphSolver]
    val context = logicalPlanningContext(strategy)
    val otherSide = newMockedLogicalPlan(context.planningAttributes, "anon_1")
    mockStrategyWithMultiplePlans(strategy, otherSide)

    val source = newMockedLogicalPlan(context.planningAttributes, "a")

    // when
    val solver = PatternExpressionSolver.solverFor(source, context)
    val expressions = Map("x" -> patExpr1).map{ case (k,v) => (k, removeGeneratedNamesAndParamsOnTree(solver.solve(v, Some(k)))) }
    val resultPlan = removeGeneratedNamesAndParamsOnTree(solver.rewrittenPlan())

    // then
    resultPlan should beLike {
      case RollUpApply(`source`,
      Projection(`otherSide`, MapKeys("anon_0")),
      "x", "anon_0") => ()
    }
    expressions should equal(Map("x" -> varFor("x")))
  }

  private def mockStrategyWithMultiplePlans(strategy: QueryGraphSolver, plans: LogicalPlan*): Unit = {
    val planIter = plans.iterator
    when(strategy.plan(any(), any(), any())).thenAnswer(new Answer[BestResults[LogicalPlan]] {
      override def answer(invocation: InvocationOnMock): BestResults[LogicalPlan] = {
        BestResults(planIter.next(), None)
      }
    })
  }

  test("Rewrites multiple pattern expressions") {
    // given MATCH (a) RETURN (a)-->(b) as x, (a)<--(b) as y
    val strategy = mock[QueryGraphSolver]
    val context = logicalPlanningContext(strategy)
    val b1 = newMockedLogicalPlan(context.planningAttributes, "outgoing-inner-plan")
    val b2 = newMockedLogicalPlan(context.planningAttributes, "incoming-inner-plan")
    mockStrategyWithMultiplePlans(strategy, b1, b2)
    val source = newMockedLogicalPlan(context.planningAttributes, "a")

    val solver = PatternExpressionSolver.solverFor(source, context)
    val expressions = Map("x" -> patExpr1, "y" -> patExpr2).map{ case (k,v) => (k, removeGeneratedNamesAndParamsOnTree(solver.solve(v, Some(k)))) }
    val resultPlan = removeGeneratedNamesAndParamsOnTree(solver.rewrittenPlan())

    // then
    resultPlan should beLike {
      case RollUpApply(
      RollUpApply(`source`,
      Projection(`b1`, MapKeys("anon_0")),
      "x", "anon_0"),
      Projection(`b2`, MapKeys("anon_3")),
      "y", "anon_3") => ()
    }
    expressions should equal(Map("x" -> varFor("x"), "y" -> varFor("y")))
  }

  test("Rewrites pattern expression inside complex expression") {
    // given MATCH (a) RETURN (a)-->() = (a)--()
    val strategy = mock[QueryGraphSolver]
    val context = logicalPlanningContext(strategy)
    val b1 = newMockedLogicalPlan(context.planningAttributes, "outgoing-inner-plan")
    val b2 = newMockedLogicalPlan(context.planningAttributes, "both-inner-plan")
    mockStrategyWithMultiplePlans(strategy, b1, b2)
    val source = newMockedLogicalPlan(context.planningAttributes, "a")

    val stringToEquals1 = Map("x" -> equals(patExpr1, patExpr2))
    val solver = PatternExpressionSolver.solverFor(source, context)
    val expressions = stringToEquals1.map{ case (k,v) => (k, removeGeneratedNamesAndParamsOnTree(solver.solve(v, Some(k)))) }
    val resultPlan = removeGeneratedNamesAndParamsOnTree(solver.rewrittenPlan())

    // then
    resultPlan should beLike {
      case RollUpApply(
      RollUpApply(`source`,
      Projection(`b1`, MapKeys("anon_0")),
      "anon_0", "anon_0"),
      Projection(`b2`, MapKeys("anon_3")),
      "anon_3", "anon_3") => ()
    }

    expressions should equal(Map("x" -> equals(varFor("anon_0"), varFor("anon_3"))))
  }

  test("Rewrites pattern expression inside complex expression, as a WHERE predicate") {
    // given MATCH (a) WHERE (a)-->() = (a)--() return a
    val strategy = mock[QueryGraphSolver]
    val context = logicalPlanningContext(strategy)
    val b1 = newMockedLogicalPlan(context.planningAttributes, "outgoing-inner-plan")
    val b2 = newMockedLogicalPlan(context.planningAttributes, "both-inner-plan")
    mockStrategyWithMultiplePlans(strategy, b1, b2)
    val source = newMockedLogicalPlan(context.planningAttributes, "a")

    val predicate = equals(patExpr1, patExpr2)
    val solver = PatternExpressionSolver.solverFor(source, context)
    val expression = removeGeneratedNamesAndParamsOnTree(solver.solve(predicate))
    val resultPlan = removeGeneratedNamesAndParamsOnTree(solver.rewrittenPlan())

    // then
    resultPlan should beLike {
      case RollUpApply(
      RollUpApply(`source`,
      Projection(`b1`, MapKeys("anon_0")),
      "anon_0", "anon_0"),
      Projection(`b2`, MapKeys("anon_3")),
      "anon_3", "anon_3") => ()
    }

    expression should equal(equals(varFor("anon_0"), varFor("anon_3")))
  }

  private def logicalPlanningContext(strategy: QueryGraphSolver): LogicalPlanningContext =
    newMockedLogicalPlanningContext(newMockedPlanContext(), semanticTable = new SemanticTable(), strategy = strategy)

  private def newPatExpr(left: String, position: Int, rightOffset: Int, relOffset: Int, dir: SemanticDirection): PatternExpression =
    newPatExpr(left, position, Left(rightOffset), Left(relOffset), dir)

  private def newPatExpr(left: String, position: Int, rightE: Either[Int, String], relNameE: Either[Int, String], dir: SemanticDirection): PatternExpression = {
    def getNameAndPosition(rightE: Either[Int, String]) = rightE match {
      case Left(i) => (Some(varFor(s"  REL$i")), DummyPosition(i))
      case Right(name) => (Some(varFor(name)), pos)
    }

    val (right, rightPos) = getNameAndPosition(rightE)
    val (relName, relPos) = getNameAndPosition(relNameE)

    PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor(left)), Seq.empty, None) _,
      RelationshipPattern(relName, Seq.empty, None, None, dir)(relPos),
      NodePattern(right, Seq.empty, None)(rightPos)) _)(DummyPosition(position)))(Set.empty)
  }
}
