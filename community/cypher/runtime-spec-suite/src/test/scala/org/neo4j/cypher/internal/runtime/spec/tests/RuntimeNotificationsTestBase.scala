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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.AggregationSkippedNull

abstract class RuntimeNotificationsTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private def average(values: Double*): Double = values.sum / values.size

  private def sumOfMeanSquares(values: Double*): Double = {
    val m = average(values: _*)
    values.map(e => (e - m) * (e - m)).sum
  }

  private val unaryAggregations = Seq(
    ("count", 5),
    ("avg", average(1.0, 2.0, 3.0, 4.0, 5.0)),
    ("max", 5.0),
    ("min", 1.0),
    ("sum", Seq(1.0, 2.0, 3.0, 4.0, 5.0).sum),
    ("collect", java.util.List.of(1.0, 2.0, 3.0, 4.0, 5.0)),
    ("stdev", Math.sqrt(sumOfMeanSquares(1.0, 2.0, 3.0, 4.0, 5.0) / 4.0)),
    ("stdevp", Math.sqrt(sumOfMeanSquares(1.0, 2.0, 3.0, 4.0, 5.0) / 5.0))
  )

  private val binaryAggregations = Seq(
    ("percentileCont", 3.0),
    ("percentileDisc", 3.0)
  )

  unaryAggregations.foreach {
    case (aggregationFunction, expected) =>
      test(s"$aggregationFunction should warn when encountering null") {
        // given empty db

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("x")
          .aggregation(Seq.empty, Seq(s"$aggregationFunction(i) AS x"))
          .unwind("[1.0, 2.0, NULL, NULL, 3.0, 4.0, NULL, 5.0] AS i")
          .argument()
          .build()

        val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)

        // then
        runtimeResult should beColumns("x")
          .withSingleRow(expected)
          .withNotifications(AggregationSkippedNull)
      }

      test(s"$aggregationFunction should not warn when not encountering null") {
        // given empty db

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("x")
          .aggregation(Seq.empty, Seq(s"$aggregationFunction(i) AS x"))
          .unwind("[1.0, 2.0, 3.0, 4.0, 5.0] AS i")
          .argument()
          .build()

        val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)

        // then
        runtimeResult should beColumns("x")
          .withSingleRow(expected)
          .withNoNotifications()
      }
  }

  binaryAggregations.foreach {
    case (aggregationFunction, expected) =>
      test(s"$aggregationFunction should warn when encountering null") {
        // given empty db

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("x")
          .aggregation(Seq.empty, Seq(s"$aggregationFunction(i, 0.5) AS x"))
          .unwind("[1, 2, NULL, NULL, 3, 4, NULL, 5] AS i")
          .argument()
          .build()

        val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)

        // then
        runtimeResult should beColumns("x")
          .withSingleRow(expected)
          .withNotifications(AggregationSkippedNull)
      }
      test(s"$aggregationFunction should not warn when not encountering null") {
        // given empty db

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("x")
          .aggregation(Seq.empty, Seq(s"$aggregationFunction(i, 0.5) AS x"))
          .unwind("[1, 2, 3, 4, 5] AS i")
          .argument()
          .build()

        val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)

        // then
        runtimeResult should beColumns("x")
          .withSingleRow(expected)
          .withNoNotifications()
      }
  }
}
