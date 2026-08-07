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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropLtV_int
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.planner.spi.histogram.Histogram
import org.neo4j.cypher.internal.util.Selectivity

class HistogramTest extends CypherPlannerTestSuite {

  private def testHistogram: Histogram = {
    HistogramTestHelper.createHistogramFromString(
      NODE_TYPE,
      "Person",
      "prop",
      """
        |+-------------------------------------------------------+
        || minInclusive | maxExclusive | frequency | selectivity |
        |+-------------------------------------------------------+
        || 1960         | 1962         | 2         | 0.02        |
        || 1962         | 1963         | 92        | 0.92        |
        || 1963         | 1987         | 2         | 0.02        |
        || 1987         | 1989         | 2         | 0.02        |
        || 1989         | 1990         | 2         | 0.02        |
        |+-------------------------------------------------------+
        |""".stripMargin
    )
  }

  test("n.prop < 1960: no buckets in range") {
    EstimateSelectivityUsingHistogram.sumBucketSelectivityEstimates(
      testHistogram,
      nPropLtV_int(1960)
    ) shouldBe Selectivity(0.0)
  }

  test("n.prop < 60: no buckets in range") {
    EstimateSelectivityUsingHistogram.sumBucketSelectivityEstimates(
      testHistogram,
      nPropLtV_int(60)
    ) shouldBe Selectivity(0.0)
  }

  test("n.prop < 1990: all buckets in range") {
    EstimateSelectivityUsingHistogram.sumBucketSelectivityEstimates(
      testHistogram,
      nPropLtV_int(1990)
    ) shouldBe Selectivity(1.0)
  }

  test("n.prop < 2025: all buckets in range") {
    EstimateSelectivityUsingHistogram.sumBucketSelectivityEstimates(
      testHistogram,
      nPropLtV_int(2025)
    ) shouldBe Selectivity(1.0)
  }

  test("n.prop < 1966: first two buckets full in range and third bucket partially") {
    val selectivityInRange = 0.9425 // ( 2 + 92 + ((3.0 / 24) * 2) ) / 100
    EstimateSelectivityUsingHistogram.sumBucketSelectivityEstimates(
      testHistogram,
      nPropLtV_int(1966)
    ) shouldBe Selectivity(selectivityInRange)
  }
}
