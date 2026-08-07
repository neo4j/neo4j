/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.logical.builder.SimpleLogicalPlanBuilder

class PlanningStepsSystemOutLoggerTest extends CypherPlannerTestSuite {

  private def testPlan(layers: Integer) = {
    val planBuilder = new SimpleLogicalPlanBuilder()
    // This is not how plans are built (top down), but it suffices for testing plan differences
    var builder = planBuilder.produceResults()
    for (_ <- 1 to layers) {
      builder = builder.projection()
    }
    builder.input().build()
  }

  test("should log simple call without change") {
    val logger = PlanningStepsSystemOutLogger()
    val plan = testPlan(0)
    logger.startFunction("FunctionA", plan)
    logger.stopFunction(plan)
    logger.flushFunctionLog() should equal("[FunctionA]")
  }

  test("should log simple call with change") {
    val logger = PlanningStepsSystemOutLogger()
    val plan1 = testPlan(0)
    val plan2 = testPlan(1)
    logger.startFunction("FunctionA", plan1)
    logger.stopFunction(plan2)
    logger.flushFunctionLog() should equal("FunctionA")
  }

  test("should log concatenated calls") {
    val logger = PlanningStepsSystemOutLogger()
    val plan1 = testPlan(0)
    val plan2 = testPlan(1)
    logger.startFunction("FunctionA", plan1)
    logger.stopFunction(plan2)
    logger.startFunction("FunctionB", plan2)
    logger.stopFunction(plan2)
    logger.flushFunctionLog() should equal("FunctionA -> [FunctionB]")
  }

  test("should log complex nested calls") {
    val logger = PlanningStepsSystemOutLogger()

    val plan1 = testPlan(0)
    val plan2 = testPlan(1)
    val plan3 = testPlan(2)

    logger.startFunction("FunctionA", plan1)
    logger.stopFunction(plan2)
    logger.startFunction("FunctionB", plan2)
    logger.startFunction("FunctionC", plan2)
    logger.stopFunction(plan2)
    logger.startFunction("FunctionD", plan2)
    logger.stopFunction(plan2)
    logger.stopFunction(plan2)
    logger.startFunction("FunctionE", plan2)
    logger.startFunction("FunctionF", plan2)
    logger.stopFunction(plan2)
    logger.startFunction("FunctionG", plan2)
    logger.stopFunction(plan3)
    logger.stopFunction(plan3)

    logger.flushFunctionLog() should equal(
      "FunctionA -> [FunctionB([FunctionC] -> [FunctionD])] -> FunctionE([FunctionF] -> FunctionG)"
    )
  }
}
