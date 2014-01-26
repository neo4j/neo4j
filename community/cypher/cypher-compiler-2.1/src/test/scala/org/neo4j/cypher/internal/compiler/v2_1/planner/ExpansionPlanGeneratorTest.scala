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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.Direction
import org.mockito.Mockito._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExpansionPlanGeneratorTest extends FunSuite with MockitoSugar with PlanGeneratorTest {

  val planContext = mock[PlanContext]
  val calculator = mock[CostCalculator]
  val estimator = mock[CardinalityEstimator]
  val generator = new ExpansionPlanGenerator(calculator, estimator)

  test("single node pattern") {
    // MATCH (a) RETURN a
    // GIVEN
    val GIVEN = table(
      plan(Set(0), "plan0", 10)
    )
    val queryGraph = QueryGraph(Id(0), Seq.empty, Seq.empty, Seq.empty)

    // WHEN
    val resultPlan = expand(queryGraph, GIVEN)

    // THEN
    assert(resultPlan === GIVEN)
  }

  test("two nodes") {
    // MATCH (a) RETURN a
    // GIVEN
    val GIVEN = table(
      plan(Set(0), "plan0", 10),
      plan(Set(1), "plan1", 10)
    )
    when(calculator.costForExpandRelationship(666)).thenReturn(Cost(0, 0))
    val queryGraph = QueryGraph(Id(0), Seq(GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq.empty)), Seq.empty, Seq.empty)

    // WHEN
    val resultPlanTable = expand(queryGraph, GIVEN)

    /* Then
    IDs  name  cost
    [0]  plan0 10
    [1]  plan1 10
    [0,1]  plan2 15
    [0,1]  plan3 15
    */

    val last = resultPlanTable.plans.last
    val nextToLast = resultPlanTable.plans.apply(2)

    assert(resultPlanTable.size === 4)
    assert(last.coveredIds === Set(Id(0), Id(1)))
    assert(nextToLast.coveredIds === Set(Id(0), Id(1)))

    assert(nextToLast.isInstanceOf[ExpandRelationships], "Expected an expansion to have been added to the plan")
    assert(last.isInstanceOf[ExpandRelationships], "Expected an expansion to have been added to the plan")
  }

  test("two nodes without a relationship (cartesian)") {
    // MATCH (a) RETURN a
    // GIVEN
    val GIVEN = table(
      plan(Set(0), "plan0", 10),
      plan(Set(1), "plan1", 10)
    )
    when(calculator.costForExpandRelationship(666)).thenReturn(Cost(0, 0))
    val queryGraph = QueryGraph(Id(0), Seq.empty, Seq.empty, Seq.empty)

    // WHEN
    val resultPlanTable = expand(queryGraph, GIVEN)

    // THEN

    assert(resultPlanTable === GIVEN)
  }

  private def expand(queryGraph: QueryGraph, GIVEN: PlanTable): PlanTable =
    generator.generatePlan(planContext, queryGraph, GIVEN)
}
