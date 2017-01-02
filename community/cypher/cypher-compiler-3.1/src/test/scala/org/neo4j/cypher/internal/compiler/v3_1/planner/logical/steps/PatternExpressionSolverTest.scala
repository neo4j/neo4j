/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical.steps

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.{LogicalPlanningContext, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_1.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_1.{DummyPosition, SemanticDirection, SemanticTable}

class PatternExpressionSolverTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("Rewrites single pattern expression") {
    // given MATCH (a) RETURN (a)-->() as x
    val otherSide = newMockedLogicalPlan("  UNNAMED1")
    val strategy = createStrategy(otherSide)
    val source = newMockedLogicalPlan("a")
    val pathStep = mock[PathStep]

    val expressionSolver = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep))
    implicit val context = logicalPlanningContext(strategy)

    // when
    val (resultPlan, expressions) = expressionSolver(source, Map("x" -> patExpr1))

    // then
    val expectedInnerPlan = Projection(otherSide, Map("  FRESHID0" -> PathExpression(pathStep)(pos)))(solved)

    resultPlan should equal(RollUpApply(source, expectedInnerPlan, IdName("x"), IdName("  FRESHID0"), Set(IdName("a")))(solved))
    expressions should equal(Map("x" -> Variable("x")(pos)))
  }

  test("Rewrites multiple pattern expressions") {
    // given MATCH (a) RETURN (a)-->(b) as x, (a)<--(b) as y
    val b1 = newMockedLogicalPlan("outgoing-inner-plan")
    val b2 = newMockedLogicalPlan("incoming-inner-plan")
    val strategy = createStrategy(b1, b2)
    implicit val context = logicalPlanningContext(strategy)
    val source = newMockedLogicalPlan("a")
    val pathStep1 = mock[PathStep]
    val pathStep2 = mock[PathStep]

    // when
    val expressionSolver = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep1, namedPatExpr2 -> pathStep2))

    val (resultPlan, expressions) = expressionSolver(source, Map("x" -> patExpr1, "y" -> patExpr2))

    // then
    val expectedInnerPlan1 = Projection(b1, Map("  FRESHID0" -> PathExpression(pathStep1)(pos)))(solved)
    val rollUp1 = RollUpApply(source, expectedInnerPlan1, IdName("x"), IdName("  FRESHID0"), Set(IdName("a")))(solved)

    val expectedInnerPlan2 = Projection(b2, Map("  FRESHID3" -> PathExpression(pathStep2)(pos)))(solved)
    val rollUp2 = RollUpApply(rollUp1, expectedInnerPlan2, IdName("y"), IdName("  FRESHID3"), Set(IdName("a")))(solved)

    resultPlan should equal(rollUp2)
    expressions should equal(Map("x" -> Variable("x")(pos), "y" -> Variable("y")(pos)))
  }

  test("Rewrites pattern expression inside complex expression") {
    // given MATCH (a) RETURN (a)-->() = (a)--()
    val b1 = newMockedLogicalPlan("outgoing-inner-plan")
    val b2 = newMockedLogicalPlan("both-inner-plan")
    val strategy = createStrategy(b1, b2)
    implicit val context = logicalPlanningContext(strategy)
    val source = newMockedLogicalPlan("a")
    val pathStep1 = mock[PathStep]
    val pathStep2 = mock[PathStep]

    // when
    val expressionSolver = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep1, namedPatExpr2 -> pathStep2))

    val stringToEquals1: Map[String, Expression] = Map("x" -> Equals(patExpr1, patExpr2)(pos))
    val (resultPlan, expressions) = expressionSolver(source, stringToEquals1)

    // then
    val expectedInnerPlan1 = Projection(b1, Map("  FRESHID0" -> PathExpression(pathStep1)(pos)))(solved)
    val rollUp1 = RollUpApply(source, expectedInnerPlan1, IdName("  FRESHID1"), IdName("  FRESHID0"), Set(IdName("a")))(solved)

    val expectedInnerPlan2 = Projection(b2, Map("  FRESHID3" -> PathExpression(pathStep2)(pos)))(solved)
    val rollUp2 = RollUpApply(rollUp1, expectedInnerPlan2, IdName("  FRESHID4"), IdName("  FRESHID3"), Set(IdName("a")))(solved)

    resultPlan should equal(rollUp2)
    expressions should equal(Map("x" -> Equals(Variable("  FRESHID1")(pos), Variable("  FRESHID4")(pos))(pos)))
  }

  test("Rewrites pattern expression inside complex expression, as a WHERE predicate") {
    // given MATCH (a) WHERE (a)-->() = (a)--() return a
    val b1 = newMockedLogicalPlan("outgoing-inner-plan")
    val b2 = newMockedLogicalPlan("both-inner-plan")
    val strategy = createStrategy(b1, b2)
    implicit val context = logicalPlanningContext(strategy)
    val source = newMockedLogicalPlan("a")
    val pathStep1 = mock[PathStep]
    val pathStep2 = mock[PathStep]

    // when
    val expressionSolver = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep1, namedPatExpr2 -> pathStep2))

    val predicate = Equals(patExpr1, patExpr2)(pos)
    val (resultPlan, expressions) = expressionSolver(source, Seq(predicate))

    // then
    val expectedInnerPlan1 = Projection(b1, Map("  FRESHID0" -> PathExpression(pathStep1)(pos)))(solved)
    val rollUp1 = RollUpApply(source, expectedInnerPlan1, IdName("  FRESHID1"), IdName("  FRESHID0"), Set(IdName("a")))(solved)

    val expectedInnerPlan2 = Projection(b2, Map("  FRESHID3" -> PathExpression(pathStep2)(pos)))(solved)
    val rollUp2 = RollUpApply(rollUp1, expectedInnerPlan2, IdName("  FRESHID4"), IdName("  FRESHID3"), Set(IdName("a")))(solved)

    resultPlan should equal(rollUp2)
    expressions should equal(Seq(Equals(Variable("  FRESHID1")(pos), Variable("  FRESHID4")(pos))(pos)))
  }


  private def logicalPlanningContext(strategy: QueryGraphSolver) =
    newMockedLogicalPlanningContext(newMockedPlanContext, strategy = strategy, semanticTable = new SemanticTable())

  private def createPatternExpressionBuilder(pathSteps: Map[PatternExpression, PathStep]) =
    PatternExpressionSolver(pathSteps.map {
      case (exp, step) => EveryPath(exp.pattern.element) -> step
    })

  private val patExpr1 = newPatExpr("a", 0, 1, 2, SemanticDirection.OUTGOING)
  private val patExpr2 = newPatExpr("a", 3, 4, 5, SemanticDirection.INCOMING)
  private val namedPatExpr1 = newPatExpr("a", 0, Right("  UNNAMED2"), Right("  UNNAMED3"), SemanticDirection.OUTGOING)
  private val namedPatExpr2 = newPatExpr("a", 3, Right("  UNNAMED5"), Right("  UNNAMED6"), SemanticDirection.INCOMING)

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
      RelationshipPattern(relName, optional = false, Seq.empty, None, None, dir)(relPos),
      NodePattern(right, Seq.empty, None)(rightPos)) _)(DummyPosition(position)))
  }

  private def createStrategy(plan: LogicalPlan*): QueryGraphSolver = {
    val strategy = mock[QueryGraphSolver]
    when(strategy.plan(any[QueryGraph])(any[LogicalPlanningContext], any())).thenReturn(plan.head, plan.tail:_*)
    strategy
  }
}
