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

import org.apache.commons.io.FileUtils
import org.neo4j.cypher.graphcounts.GraphCountsJson
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class BenchmarkCardinalityEstimationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  private val qmulGraphCounts = GraphCountsJson.parseAsGraphCountData(
    FileUtils.toFile(classOf[BenchmarkCardinalityEstimationTest].getResource("/qmul.json"))
  )

  test("qmul should estimate simple pattern relationship with labels and rel type") {
    val planner = plannerBuilder()
      .processGraphCounts(qmulGraphCounts)
      .build()

    val query = "MATCH (a:Person)-[r:friends]->(b:Person) RETURN r"
    val planState = planner.planState(query)
    val plan = planState.logicalPlan
    val cardinalities = planState.planningAttributes.cardinalities

    val actual = 4535800.0
    val expectedDifference = 900.0
    val allowedSlack = 0.05

    val estimate = cardinalities.get(plan.id).amount
    val currentDifference = Math.abs(estimate - actual)

    val differenceDifference = Math.abs(currentDifference - expectedDifference)
    val differenceDifferenceMargin = Math.round(expectedDifference * allowedSlack)

    if (differenceDifference > differenceDifferenceMargin) {
      val builder = new StringBuilder
      builder.append(s"Estimate changed by more than ${allowedSlack * 100} % for query $query:\n")
      builder.append(s" - actual cardinality: $actual\n")
      builder.append(s" - estimated cardinality: $estimate\n")
      builder.append(s" - expected difference: $expectedDifference ± ${differenceDifferenceMargin / 2}\n")
      builder.append(s" - actual difference: $currentDifference\n")

      fail(builder.toString())
    }
  }
}
