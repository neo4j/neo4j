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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, Selection}

class SelectCoveredTest extends CypherFunSuite with LogicalPlanningTestSupport with AstConstructionTestSupport {
  private implicit val planContext = newMockedPlanContext
  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]
  private implicit val context = newMockedLogicalPlanningContext(planContext)

  test("when a predicate that isn't already solved is solvable it should be applied") {
    // Given
    val predicate = Equals(literalInt(10), literalInt(10))(pos)
    val inner = newMockedLogicalPlan("x")
    val selections = Selections(Set(Predicate(inner.availableSymbols, predicate)))

    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(inner, qg)

    // Then
    result should equal(Seq(Selection(Seq(predicate), inner)(solved)))
  }

  test("should not try to solve predicates with unmet dependencies") {
    // Given
    val predicate = propEquality(variable = "n", propKey = "prop", intValue = 42)

    val selections = Selections.from(predicate)
    val inner = newMockedLogicalPlanWithProjections("x")

    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(inner, qg)

    // Then
    result should be (empty)
  }

  test("when two predicates not already solved are solvable, they should be applied") {
    // Given
    val predicate1 = Equals(literalInt(10), literalInt(10))(pos)
    val predicate2 = Equals(literalInt(30), literalInt(10))(pos)

    val selections = Selections.from(Seq(predicate1, predicate2))
    val inner: LogicalPlan = newMockedLogicalPlanWithProjections("x")

    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(inner, qg)

    // Then
    result should equal(Seq(Selection(Seq(predicate1, predicate2), inner)(solved)))
  }

  test("when a predicate is already solved, it should not be applied again") {
    // Given
    val coveredIds = Set(IdName("x"))
    val qg = QueryGraph(selections = Selections(Set(Predicate(coveredIds, SignedDecimalIntegerLiteral("1") _))))
    val solved = CardinalityEstimation.lift(RegularPlannerQuery(qg), 0.0)
    val inner = newMockedLogicalPlanWithProjections("x").updateSolved(solved)

    // When
    val result = selectCovered(inner, qg)

    // Then
    result should equal(Seq())
  }

  test("a predicate without all dependencies covered should not be applied ") {
    // Given
    val predicate = mock[Expression]
    val selections = Selections(Set(Predicate(Set(IdName("x"), IdName("y")), predicate)))
    val inner = newMockedLogicalPlanWithProjections("x")
    val qg = QueryGraph(selections = selections)

    // When
    val result = selectCovered(inner, qg)

    // Then
    result should equal(Seq())
  }
}
