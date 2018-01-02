/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningConfiguration, QueryGraph, LogicalPlanningTestSupport2}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.dbstructure.QMULDbStructure

class BenchmarkCardinalityEstimationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val qmul: LogicalPlanningEnvironment[_] = new fromDbStructure(QMULDbStructure.INSTANCE)

  test("qmul should estimate simple pattern relationship with labels and rel type") {
    qmul.assertNoRegression("MATCH (a:Person)-[r:friends]->(b:Person) RETURN r", 4535800.0, 900.0)
  }

  implicit class RichLogicalPlanningEnvironment(val env: LogicalPlanningEnvironment[_]) {
    def assertNoRegression(query: String, actual: Double, expectedDifference: Double, allowedSlack: Double = 0.05): Unit = {
      val plan = env.planFor(query)
      val qg = plan.plan.solved.graph
      val estimate = env.estimate(qg).amount
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

        // println(builder)
        // println()

        throw new AssertionError(builder.toString())
      }
    }
  }
}
