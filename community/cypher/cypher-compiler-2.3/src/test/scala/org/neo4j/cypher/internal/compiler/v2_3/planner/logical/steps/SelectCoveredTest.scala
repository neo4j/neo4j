/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.mockito.Mockito._
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SelectCoveredTest extends CypherFunSuite with LogicalPlanningTestSupport {
  private implicit val planContext = newMockedPlanContext
  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]
  private implicit val context = newMockedLogicalPlanningContext(planContext)

  test("when a predicate that isn't already solved is solvable it should be applied") {
    // Given
    val predicate = mock[Expression]
    when(predicate.dependencies).thenReturn(Set.empty[Identifier])
    val LogicalPlan = newMockedLogicalPlan("x")
    val selections = Selections(Set(Predicate(LogicalPlan.availableSymbols, predicate)))

    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(LogicalPlan, qg)

    // Then
    result should equal(Seq(Selection(Seq(predicate), LogicalPlan)(solved)))
  }

  test("should not try to solve predicates with unmet dependencies") {
    // Given
    val predicate = mock[Expression]
    when(predicate.dependencies).thenReturn(Set.empty[Identifier])

    val selections = Selections(Set(Predicate(Set(IdName("x")), predicate)))
    val LogicalPlan = newMockedLogicalPlanWithProjections("x")

    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(LogicalPlan, qg)

    // Then
    result should equal(Seq(Selection(Seq(predicate), LogicalPlan)(solved)))
  }

  test("when two predicates not already solved are solvable, they should be applied") {
    // Given
    val predicate1 = mock[Expression]
    when(predicate1.dependencies).thenReturn(Set.empty[Identifier])

    val predicate2 = mock[Expression]
    when(predicate2.dependencies).thenReturn(Set.empty[Identifier])

    val selections = Selections(Set(
      Predicate(Set(IdName("x")), predicate1),
      Predicate(Set(IdName("x")), predicate2)))
    val LogicalPlan: LogicalPlan = newMockedLogicalPlanWithProjections("x")

    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(LogicalPlan, qg)

    // Then
    result should equal(Seq(Selection(Seq(predicate1, predicate2), LogicalPlan)(solved)))
  }

  test("when a predicate is already solved, it should not be applied again") {
    // Given
    val coveredIds = Set(IdName("x"))
    val qg = QueryGraph(selections = Selections(Set(Predicate(coveredIds, SignedDecimalIntegerLiteral("1") _))))
    val solved = CardinalityEstimation.lift(PlannerQuery(qg), 0.0)
    val LogicalPlan = newMockedLogicalPlanWithProjections("x").updateSolved(solved)

    // When
    val result = selectCovered(LogicalPlan, qg)

    // Then
    result should equal(Seq())
  }

  test("a predicate without all dependencies covered should not be applied ") {
    // Given
    val predicate = mock[Expression]
    val selections = Selections(Set(Predicate(Set(IdName("x"), IdName("y")), predicate)))
    val LogicalPlan = newMockedLogicalPlanWithProjections("x")
    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(LogicalPlan, qg)

    // Then
    result should equal(Seq())
  }
}
