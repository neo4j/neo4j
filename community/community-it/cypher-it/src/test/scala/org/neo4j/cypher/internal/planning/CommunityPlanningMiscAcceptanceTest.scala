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
package org.neo4j.cypher.internal.planning

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.util.DontRunOnSpdBuild

class CommunityPlanningMiscAcceptanceTest extends ExecutionEngineFunSuite with DontRunOnSpdBuild {

  test(
    "community planner should estimate cardinality of cartesian product RHS using volcano model regardless of runtime option"
  ) {
    val nodeCount = 100
    givenTx {
      for (_ <- 1 to nodeCount)
        createNode()
    }

    for {
      runtimeOptionName <- CypherRuntimeOption.values.map(_.name)
    } withClue(runtimeOptionName) {

      val query =
        s"""CYPHER runtime=${runtimeOptionName}
           |EXPLAIN
           |MATCH (a), (b)
           |RETURN a, b
           |""".stripMargin

      val result = execute(query, Map.empty)
      result.executionPlanDescription() should
        includeSomewhere
          .aPlan("CartesianProduct")
          .withRHS(
            aPlan("AllNodesScan").withEstimatedRows(nodeCount * nodeCount)
          )
    }
  }
}
