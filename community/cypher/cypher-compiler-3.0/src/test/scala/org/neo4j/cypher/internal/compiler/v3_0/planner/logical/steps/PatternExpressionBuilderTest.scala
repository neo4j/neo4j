/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_0.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_0.{DummyPosition, SemanticDirection, SemanticTable}

class PatternExpressionBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("Rewrites single pattern expression") {
    // given MATCH (a) RETURN (a)-->() as x
    val otherSide = newMockedLogicalPlan("  UNNAMED1")
    val strategy = createStrategy(otherSide)
    val source = newMockedLogicalPlan("a")
    val pathStep = mock[PathStep]

    val expressionsBuilder = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep))
    implicit val context = logicalPlanningContext(strategy)

    // when
    val (resultPlan, expressions) = expressionsBuilder(source, Map("x" -> patExpr1))

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
    val expressionsBuilder = createPatternExpressionBuilder(Map(namedPatExpr1 -> pathStep1, namedPatExpr2 -> pathStep2))

    val (resultPlan, expressions) = expressionsBuilder(source, Map("x" -> patExpr1, "y" -> patExpr2))

    // then
    val expectedInnerPlan1 = Projection(b1, Map("  FRESHID0" -> PathExpression(pathStep1)(pos)))(solved)
    val rollUp1 = RollUpApply(source, expectedInnerPlan1, IdName("x"), IdName("  FRESHID0"), Set(IdName("a")))(solved)

    val expectedInnerPlan2 = Projection(b2, Map("  FRESHID0" -> PathExpression(pathStep2)(pos)))(solved)
    val rollUp2 = RollUpApply(rollUp1, expectedInnerPlan2, IdName("y"), IdName("  FRESHID0"), Set(IdName("a")))(solved)

    resultPlan should equal(rollUp2)
    expressions should equal(Map("x" -> Variable("x")(pos), "y" -> Variable("y")(pos)))
  }

  private def logicalPlanningContext(strategy: QueryGraphSolver) =
    newMockedLogicalPlanningContext(newMockedPlanContext, strategy = strategy, semanticTable = new SemanticTable())

  private def createPatternExpressionBuilder(pathSteps: Map[PatternExpression, PathStep]) =
    patternExpressionBuilder(pathSteps.map {
      case (exp, step) => EveryPath(exp.pattern.element) -> step
    })

  private val patExpr1 = newPatExpr("a", 0, 1)
  private val patExpr2 = newPatExpr("a", 0, 1, SemanticDirection.INCOMING)
  private val namedPatExpr1 = newPatExpr("a", Right("  UNNAMED1"), Right("  UNNAMED2"), SemanticDirection.OUTGOING)
  private val namedPatExpr2 = newPatExpr("a", Right("  UNNAMED1"), Right("  UNNAMED2"), SemanticDirection.INCOMING)

  private def newPatExpr(left: String, rightOffset: Int, relOffset: Int, dir: SemanticDirection = SemanticDirection.OUTGOING): PatternExpression = newPatExpr(left, Left(rightOffset), Left(relOffset), dir)
  private def newPatExpr(left: String, right: String, relOffset: Int): PatternExpression = newPatExpr(left, Right(right), Left(relOffset), SemanticDirection.INCOMING)
  private def newPatExpr(left: String, right: String, relOffset: Int, dir: SemanticDirection): PatternExpression = newPatExpr(left, Right(right), Left(relOffset), dir)
  private def newPatExpr(left: String, rightE: Either[Int, String], relNameE: Either[Int, String], dir: SemanticDirection): PatternExpression = {

    def getNameAndPosition(rightE: Either[Int, String]) = rightE match {
      case (Left(i)) => (None, DummyPosition(i))
      case (Right(name)) => (Some(Variable(name)(pos)), pos)
    }

    val (right, rightPos) = getNameAndPosition(rightE)
    val (relName, relPos) = getNameAndPosition(relNameE)

    PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor(left)), Seq.empty, None) _,
      RelationshipPattern(relName, optional = false, Seq.empty, None, None, dir)(relPos),
      NodePattern(right, Seq.empty, None)(rightPos)) _) _)
  }

  private def createStrategy(plan: LogicalPlan*): QueryGraphSolver = {
    val strategy = mock[QueryGraphSolver]
    when(strategy.plan(any[QueryGraph])(any[LogicalPlanningContext], any())).thenReturn(plan.head, plan.tail:_*)
    strategy
  }
}
