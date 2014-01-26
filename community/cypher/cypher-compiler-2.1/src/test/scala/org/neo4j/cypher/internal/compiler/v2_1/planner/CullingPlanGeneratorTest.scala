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
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class CullingPlanGeneratorTest extends FunSuite with PlanGeneratorTest {
  val planGenerator = CullingPlanGenerator()

  test("empty plan returns empty") {
    val planTable = PlanTable.empty

    assert(cull(planTable) === planTable)
  }

  test("single plan is not culled") {
    val GIVEN = table(
      plan(Set(0), "plan1", 5)
    )

    val generatedPlanTable = cull(GIVEN)

    // Expected the same table back
    assert(generatedPlanTable === GIVEN)
  }

  test("two plans not covering each other are not culled") {
    val GIVEN = table(
      plan(Set(0), "plan1", 5),
      plan(Set(1), "plan2", 5)
    )

    val generatedPlanTable = cull(GIVEN)

    // Expected the same table back
    assert(generatedPlanTable === GIVEN)
  }

  test("two plans covering each other returns the cheaper of the two") {
    val GIVEN = table(
      plan(Set(0), "plan1", 10),
      plan(Set(0), "plan2", 5)
    )

    // When
    val generatedPlanTable = cull(GIVEN)

    // Then
    val EXPECTED = table(
      plan(Set(0), "plan2", 5)
    )

    assert(generatedPlanTable === EXPECTED)
  }

  test("two plans overlapping but not covering each other are not culled") {
    val GIVEN = table(
      plan(Set(0,1), "plan1", 5),
      plan(Set(1,2), "plan2", 10)
    )

    // When
    val generatedPlanTable = cull(GIVEN)

    // Then
    assert(generatedPlanTable === GIVEN)
  }

  test("plan table culls the expected plans") {
    /*Query: MATCH (a)-->(b)-->(c)*/
    val GIVEN = table(
      plan(Set(0),   "plan1", 5),
      plan(Set(1),   "plan2", 10),
      plan(Set(0,1), "plan3", 12),
      plan(Set(1,2), "plan4", 12),
      plan(Set(0,2), "plan5", 15),
      plan(Set(0,1), "plan6", 20)
    )

    // When
    val generatedPlanTable = cull(GIVEN)

    // Then
    val EXPECTED = table(
      plan(Set(0,1), "plan3", 12),
      plan(Set(1,2), "plan4", 12),
      plan(Set(0,2), "plan5", 15)
    )

    assert(generatedPlanTable === EXPECTED)
  }

  private def cull(planTable: PlanTable) = planGenerator.generatePlan(null, null, planTable)
}
