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
import org.neo4j.cypher.internal.compiler.v2_1.planner.{Predicate, QueryGraph, Selections, LogicalPlanningTestSupport}
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.planner.Predicate

class SelectCoveredTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("when a predicate that isn't already solved is solvable it should be applied") {
    // Given
    implicit val planContext = newMockedPlanContext
    implicit val context = newMockedLogicalPlanContext(planContext)
    val queryPlan: QueryPlan = newMockedLogicalPlan("x")
    val predicate = mock[Expression]
    val selections = Selections(Set(Predicate(queryPlan.plan.coveredIds, predicate)))
    when(context.queryGraph.selections).thenReturn(selections)

    // When
    val result = selectCovered(queryPlan)

    // Then
    result should equal(SelectionPlan(Seq(predicate), queryPlan))
  }

  test("should not try to solve predicates with unmet dependencies") {
    // Given
    implicit val planContext = newMockedPlanContext
    implicit val context = newMockedLogicalPlanContext(planContext)
    val queryPlan = newMockedQueryPlan("x")
    val predicate = mock[Expression]
    val selections = Selections(Set(Predicate(queryPlan.plan.coveredIds, predicate)))
    when(context.queryGraph.selections).thenReturn(selections)

    // When
    val result = selectCovered(queryPlan)

    // Then
    result should equal(SelectionPlan(Seq(predicate), queryPlan))
  }

  test("when two predicates not already solved are solvable, they should be applied") {
    // Given
    implicit val planContext = newMockedPlanContext
    implicit val context = newMockedLogicalPlanContext(planContext)
    val queryPlan = newMockedQueryPlan("x")
    val plan = queryPlan.plan
    val predicate1 = mock[Expression]
    val predicate2 = mock[Expression]
    val selections = Selections(Set(
      Predicate(plan.coveredIds, predicate1),
      Predicate(plan.coveredIds, predicate2)))
    when(context.queryGraph.selections).thenReturn(selections)

    // When
    val result = selectCovered(queryPlan)

    // Then
    result should equal(SelectionPlan(Seq(predicate1, predicate2), queryPlan))
  }

  test("when a predicate that is already solved, it should not be applied again") {
    // Given
    implicit val planContext = newMockedPlanContext
    implicit val context = newMockedLogicalPlanContext(planContext)
    val queryPlan = newMockedQueryPlan("x")
    val predicate = mock[Expression]
    val selections = Selections(Set(Predicate(queryPlan.plan.coveredIds, predicate)))
    when(context.queryGraph.selections).thenReturn(selections)
    val selectionsQG = QueryGraph(selections = selections)
    when(queryPlan.plan.solved).thenReturn(selectionsQG)

    // When
    val result = selectCovered(queryPlan.plan)

    // Then
    result should equal(QueryPlan(queryPlan.plan, selectionsQG))
  }

  test("a predicate without all dependencies covered should not be applied ") {
    // Given
    implicit val planContext = newMockedPlanContext
    implicit val context = newMockedLogicalPlanContext(planContext)
    val queryPlan = newMockedQueryPlan("x")
    val predicate = mock[Expression]
    val selections = Selections(Set(Predicate(Set(IdName("x"), IdName("y")), predicate)))
    when(context.queryGraph.selections).thenReturn(selections)

    // When
    val result = selectCovered(queryPlan)

    // Then
    result should equal(queryPlan)
  }

}
