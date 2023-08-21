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
package org.neo4j.cypher

import org.neo4j.cypher.internal.InterpretedRuntimeName
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.SlottedRuntimeName
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.graphdb.ExecutionPlanDescription

class RootPlanAcceptanceTest extends ExecutionEngineFunSuite {

  test("cost should be default planner") {
    given("match (n) return n")
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("slotted should be default runtime") {
    given("match (n) return n")
      .shouldHaveRuntime(SlottedRuntimeName)
  }

  test("AllNodesScan should be the only child of the plan") {
    val description = given("match (n) return n").planDescription
    var children = description.getChildren
    children should have size 1
    while (children.get(0).getChildren.size() > 0) {
      children = children.get(0).getChildren
      children should have size 1
    }

    children.get(0).getName should be("AllNodesScan")
  }

  test("DbHits should contain proper values in interpreted runtime") {
    val description = given("match (n) return n")
      .withRuntime(InterpretedRuntimeName)
      .planDescription
    val children = description.getChildren
    children should have size 1
    description.getArguments.get("DbHits") should equal(0) // ProduceResults has no hits
    children.get(0).getArguments.get("DbHits") should equal(1) // AllNodesScan has 1 hit
  }

  test("Rows should be properly formatted in interpreted runtime") {
    given("match (n) return n")
      .withRuntime(InterpretedRuntimeName)
      .planDescription.getArguments.get("Rows") should equal(0)
  }

  test("EstimatedRows should be properly formatted") {
    // on missing statistics, we fake cardinality to 10
    given("MATCH (n) RETURN n").planDescription.getArguments.get("EstimatedRows") should equal(10.0)
  }

  def given(query: String) = TestQuery(query)

  case class TestQuery(
    query: String,
    planner: Option[PlannerName] = None,
    runtime: Option[RuntimeName] = None
  ) {

    lazy val planDescription: ExecutionPlanDescription = execute()

    def withPlanner(planner: PlannerName): TestQuery = copy(planner = Some(planner))

    def withRuntime(runtime: RuntimeName): TestQuery = copy(runtime = Some(runtime))

    def shouldHavePlanner(planner: PlannerName): TestQuery = {
      planDescription.getArguments.get("planner") should equal(s"${planner.toTextOutput}")
      planDescription.getArguments.get("planner-impl") should equal(s"${planner.name}")
      this
    }

    def shouldHaveRuntime(runtime: RuntimeName): TestQuery = {
      planDescription.getArguments.get("runtime") should equal(s"${runtime.toTextOutput}")
      planDescription.getArguments.get("runtime-impl") should equal(s"${runtime.name}")
      this
    }

    private def execute(): ExecutionPlanDescription = {
      graph.withTx { tx =>
        val prepend = (planner, runtime) match {
          case (None, None) => ""
          case _ =>
            val plannerString = planner.map("planner=" + _.name).getOrElse("")
            val runtimeString = runtime.map("runtime=" + _.name).getOrElse("")
            s"CYPHER $plannerString $runtimeString"
        }
        val result = executeOfficial(tx, s"$prepend PROFILE $query")
        result.resultAsString()
        result.getExecutionPlanDescription()
      }
    }
  }
}
