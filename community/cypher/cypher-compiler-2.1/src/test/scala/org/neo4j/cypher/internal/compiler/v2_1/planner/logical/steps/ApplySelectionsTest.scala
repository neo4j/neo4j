/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
/**
* Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.{Selections, LogicalPlanningTestSupport}
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{IdName, Selection}

class ApplySelectionsTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("when a predicate that isn't already solved is solvable it should be applied") {
    // Given
    val plan = newMockedLogicalPlan("x")
    val predicate = mock[Expression]
    val selections = Selections(Seq(plan.coveredIds -> predicate))
    implicit val context = newMockedLogicalPlanContext()
    when(context.queryGraph.selections).thenReturn(selections)

    // When
    val result = applySelections(plan)

    // Then
    result should equal(Selection(Seq(predicate), plan))
  }

  test("should not try to solve predicates with unmet dependencies") {
    // Given
    val plan = newMockedLogicalPlan("x")
    val predicate = mock[Expression]
    val selections = Selections(Seq(plan.coveredIds -> predicate))
    implicit val context = newMockedLogicalPlanContext()
    when(context.queryGraph.selections).thenReturn(selections)

    // When
    val result = applySelections(plan)

    // Then
    result should equal(Selection(Seq(predicate), plan))
  }

  test("when two predicates not already solved are solvable, they should be applied") {
    // Given
    val plan = newMockedLogicalPlan("x")
    val predicate1 = mock[Expression]
    val predicate2 = mock[Expression]
    val selections = Selections(Seq(plan.coveredIds -> predicate1, plan.coveredIds -> predicate2))
    implicit val context = newMockedLogicalPlanContext()
    when(context.queryGraph.selections).thenReturn(selections)

    // When
    val result = applySelections(plan)

    // Then
    result should equal(Selection(Seq(predicate1, predicate2), plan))
  }

  test("when a predicate that is already solved, it should not be applied again") {
    // Given
    val plan = newMockedLogicalPlan("x")
    val predicate = mock[Expression]
    val selections = Selections(Seq(plan.coveredIds -> predicate))
    implicit val context = newMockedLogicalPlanContext()
    when(context.queryGraph.selections).thenReturn(selections)
    when(plan.solvedPredicates).thenReturn(Seq(predicate))

    // When
    val result = applySelections(plan)

    // Then
    result should equal(plan)
  }

  test("a predicate without all dependencies covered should not be applied ") {
    // Given
    val plan = newMockedLogicalPlan("x")
    val predicate = mock[Expression]
    val selections = Selections(Seq(Set(IdName("x"), IdName("y")) -> predicate))
    implicit val context = newMockedLogicalPlanContext()
    when(context.queryGraph.selections).thenReturn(selections)

    // When
    val result = applySelections(plan)

    // Then
    result should equal(plan)
  }

}
