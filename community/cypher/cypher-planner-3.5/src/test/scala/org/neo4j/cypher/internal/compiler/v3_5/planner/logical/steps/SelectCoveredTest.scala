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

import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.expressions.{Equals, Expression, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.v3_5.logical.plans.Selection
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class SelectCoveredTest extends CypherFunSuite with LogicalPlanningTestSupport with AstConstructionTestSupport {
  private val planContext = newMockedPlanContext()

  test("when a predicate that isn't already solved is solvable it should be applied") {
    // Given
    val context = newMockedLogicalPlanningContext(planContext)

    val predicate = Equals(literalInt(10), literalInt(10))(pos)
    val inner = newMockedLogicalPlan(context.planningAttributes, "x")
    val selections = Selections(Set(Predicate(inner.availableSymbols, predicate)))

    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(inner, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(Selection(Seq(predicate), inner)))
  }

  test("should not try to solve predicates with unmet dependencies") {
    // Given
    val context = newMockedLogicalPlanningContext(planContext)

    val predicate = propEquality(variable = "n", propKey = "prop", intValue = 42)

    val selections = Selections.from(predicate)
    val inner = newMockedLogicalPlanWithProjections(context.planningAttributes, "x")

    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(inner, qg, InterestingOrder.empty, context)

    // Then
    result should be (empty)
  }

  test("when two predicates not already solved are solvable, they should be applied") {
    // Given
    val context = newMockedLogicalPlanningContext(planContext)

    val predicate1 = Equals(literalInt(10), literalInt(10))(pos)
    val predicate2 = Equals(literalInt(30), literalInt(10))(pos)

    val selections = Selections.from(Seq(predicate1, predicate2))
    val inner = newMockedLogicalPlanWithProjections(context.planningAttributes, "x")

    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(inner, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(Selection(Seq(predicate1, predicate2), inner)))
  }

  test("when a predicate is already solved, it should not be applied again") {
    // Given
    val context = newMockedLogicalPlanningContext(planContext)

    val coveredIds = Set("x")
    val qg = QueryGraph(selections = Selections(Set(Predicate(coveredIds, SignedDecimalIntegerLiteral("1") _))))
    val solved = RegularPlannerQuery(qg)
    val inner = newMockedLogicalPlanWithSolved(context.planningAttributes, idNames = Set("x"), solved = solved)

    // When
    val result = selectCovered(inner, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq())
  }

  test("a predicate without all dependencies covered should not be applied ") {
    // Given
    val context = newMockedLogicalPlanningContext(planContext)

    val predicate = mock[Expression]
    val selections = Selections(Set(Predicate(Set("x", "y"), predicate)))
    val inner = newMockedLogicalPlanWithProjections(context.planningAttributes, "x")
    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(inner, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq())
  }
}
