/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{LogicalPlanningContext, QueryGraphSolver}
import org.neo4j.cypher.internal.ir.v3_5.InterestingOrder
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan, Projection, RollUpApply}
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.DummyPosition
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class PatternExpressionSolverTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("Rewrites single pattern expression") {
    // given MATCH (a) RETURN (a)-->() as x
    val strategy = mock[QueryGraphSolver]
    val context = logicalPlanningContext(strategy)
    val pathStep = NilPathStep
    val otherSide = newMockedLogicalPlan(context.planningAttributes, "  NODE1")
    mockStrategyWithMultiplePlans(strategy, otherSide)

    val expressionSolver = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep))
    val source = newMockedLogicalPlan(context.planningAttributes, "a")

    // when
    val (resultPlan, expressions) = expressionSolver(source, Map("x" -> patExpr1), InterestingOrder.empty, context)

    // then
    val expectedInnerPlan = Projection(otherSide, Map("  FRESHID0" -> PathExpression(pathStep)(pos)))

    resultPlan should equal(RollUpApply(source, expectedInnerPlan, "x", "  FRESHID0", Set("a")))
    expressions should equal(Map("x" -> Variable("x")(pos)))
  }

  private def mockStrategyWithMultiplePlans(strategy: QueryGraphSolver, plans: LogicalPlan*): Unit = {
    val planIter = plans.iterator
    when(strategy.plan(any(), any(), any())).thenAnswer(new Answer[LogicalPlan] {
      override def answer(invocation: InvocationOnMock): LogicalPlan = {
        planIter.next()
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
    val pathStep1 = NilPathStep
    val pathStep2 = NilPathStep

    // when
    val expressionSolver = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep1, namedPatExpr2 -> pathStep2))

    val (resultPlan, expressions) = expressionSolver(source, Map("x" -> patExpr1, "y" -> patExpr2), InterestingOrder.empty, context)

    // then
    val expectedInnerPlan1 = Projection(b1, Map("  FRESHID0" -> PathExpression(pathStep1)(pos)))
    val rollUp1 = RollUpApply(source, expectedInnerPlan1, "x", "  FRESHID0", Set("a"))

    val expectedInnerPlan2 = Projection(b2, Map("  FRESHID3" -> PathExpression(pathStep2)(pos)))
    val rollUp2 = RollUpApply(rollUp1, expectedInnerPlan2, "y", "  FRESHID3", Set("a"))

    resultPlan should equal(rollUp2)
    expressions should equal(Map("x" -> Variable("x")(pos), "y" -> Variable("y")(pos)))
  }

  test("Rewrites pattern expression inside complex expression") {
    // given MATCH (a) RETURN (a)-->() = (a)--()
    val strategy = mock[QueryGraphSolver]
    val context = logicalPlanningContext(strategy)
    val b1 = newMockedLogicalPlan(context.planningAttributes, "outgoing-inner-plan")
    val b2 = newMockedLogicalPlan(context.planningAttributes, "both-inner-plan")
    mockStrategyWithMultiplePlans(strategy, b1, b2)
    val source = newMockedLogicalPlan(context.planningAttributes, "a")
    val pathStep1 = NilPathStep
    val pathStep2 = NilPathStep

    // when
    val expressionSolver = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep1, namedPatExpr2 -> pathStep2))

    val stringToEquals1: Map[String, Expression] = Map("x" -> Equals(patExpr1, patExpr2)(pos))
    val (resultPlan, expressions) = expressionSolver(source, stringToEquals1, InterestingOrder.empty, context)

    // then
    val expectedInnerPlan1 = Projection(b1, Map("  FRESHID0" -> PathExpression(pathStep1)(pos)))
    val rollUp1 = RollUpApply(source, expectedInnerPlan1, "  FRESHID1", "  FRESHID0", Set("a"))

    val expectedInnerPlan2 = Projection(b2, Map("  FRESHID3" -> PathExpression(pathStep2)(pos)))
    val rollUp2 = RollUpApply(rollUp1, expectedInnerPlan2, "  FRESHID4", "  FRESHID3", Set("a"))

    resultPlan should equal(rollUp2)
    expressions should equal(Map("x" -> Equals(Variable("  FRESHID1")(pos), Variable("  FRESHID4")(pos))(pos)))
  }

  test("Rewrites pattern expression inside complex expression, as a WHERE predicate") {
    // given MATCH (a) WHERE (a)-->() = (a)--() return a
    val strategy = mock[QueryGraphSolver]
    val context = logicalPlanningContext(strategy)
    val b1 = newMockedLogicalPlan(context.planningAttributes, "outgoing-inner-plan")
    val b2 = newMockedLogicalPlan(context.planningAttributes, "both-inner-plan")
    mockStrategyWithMultiplePlans(strategy, b1, b2)
    val source = newMockedLogicalPlan(context.planningAttributes, "a")
    val pathStep1 = NilPathStep
    val pathStep2 = NilPathStep

    // when
    val expressionSolver = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep1, namedPatExpr2 -> pathStep2))

    val predicate = Equals(patExpr1, patExpr2)(pos)
    val (resultPlan, expressions) = expressionSolver(source, Seq(predicate), InterestingOrder.empty, context)

    // then
    val expectedInnerPlan1 = Projection(b1, Map("  FRESHID0" -> PathExpression(pathStep1)(pos)))
    val rollUp1 = RollUpApply(source, expectedInnerPlan1, "  FRESHID1", "  FRESHID0", Set("a"))

    val expectedInnerPlan2 = Projection(b2, Map("  FRESHID3" -> PathExpression(pathStep2)(pos)))
    val rollUp2 = RollUpApply(rollUp1, expectedInnerPlan2, "  FRESHID4", "  FRESHID3", Set("a"))

    resultPlan should equal(rollUp2)
    expressions should equal(Seq(Equals(Variable("  FRESHID1")(pos), Variable("  FRESHID4")(pos))(pos)))
  }


  private def logicalPlanningContext(strategy: QueryGraphSolver): LogicalPlanningContext =
    newMockedLogicalPlanningContext(newMockedPlanContext, semanticTable = new SemanticTable(), strategy = strategy)

  private def createPatternExpressionBuilder(pathSteps: Map[PatternExpression, PathStep]) =
    PatternExpressionSolver(pathSteps.map {
      case (exp, step) => EveryPath(exp.pattern.element) -> step
    })

  private val patExpr1 = newPatExpr("a", 0, 1, 2, SemanticDirection.OUTGOING)
  private val patExpr2 = newPatExpr("a", 3, 4, 5, SemanticDirection.INCOMING)
  private val namedPatExpr1 = newPatExpr("a", 0, Right("  NODE2"), Right("  REL3"), SemanticDirection.OUTGOING)
  private val namedPatExpr2 = newPatExpr("a", 3, Right("  NODE5"), Right("  REL6"), SemanticDirection.INCOMING)

  private def newPatExpr(left: String, position: Int, rightOffset: Int, relOffset: Int, dir: SemanticDirection): PatternExpression =
    newPatExpr(left, position, Left(rightOffset), Left(relOffset), dir)
  private def newPatExpr(left: String, position: Int, rightE: Either[Int, String], relNameE: Either[Int, String], dir: SemanticDirection): PatternExpression = {

    def getNameAndPosition(rightE: Either[Int, String]) = rightE match {
      case (Left(i)) => (None, DummyPosition(i))
      case (Right(name)) => (Some(Variable(name)(pos)), pos)
    }

    val (right, rightPos) = getNameAndPosition(rightE)
    val (relName, relPos) = getNameAndPosition(relNameE)

    PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor(left)), Seq.empty, None) _,
      RelationshipPattern(relName, Seq.empty, None, None, dir)(relPos),
      NodePattern(right, Seq.empty, None)(rightPos)) _)(DummyPosition(position)))
  }
}
