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
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.defaultAllowedError
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropGtLt_float
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropGtLt_float_int
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropGtLt_int
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropGtLt_int_float
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropGtV_float
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropGtV_int
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropGteV_float
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropGteV_int
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropLtV_float
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropLtV_int
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropLteV_float
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram.HistogramTestHelper.nPropLteV_int
import org.neo4j.cypher.internal.planner.spi.histogram.StandardBucket

class StandardBucketEstimationTest extends CypherPlannerTestSuite {

  test("n.prop < v where v is an integer and the bucket is fully out of range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_int(5)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_int(4)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_int(0)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_int(-12)) shouldBe 0.0
  }

  test("n.prop <= v where v is an integer and the bucket is fully out of range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_int(5)) should not be 0.0 // (1/5) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_int(4)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_int(0)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_int(-12)) shouldBe 0.0
  }

  test("n.prop > v where v is an integer and the bucket is fully out of range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_int(9)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_int(10)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_int(15)) shouldBe 0.0
  }

  test("n.prop >= v where v is an integer and the bucket is fully out of range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_int(9)) should not be 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_int(10)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_int(15)) shouldBe 0.0
  }

  test("n.prop < v where v is a floating point number and the bucket is fully out of range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_float(5.0)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_float(4.5)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_float(0.0)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_float(-12.9)) shouldBe 0.0
  }

  test("n.prop <= v where v is a floating point number and the bucket is fully out of range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_float(4.9999)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_float(4.5)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_float(0.0)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_float(-12.9)) shouldBe 0.0
  }

  test("n.prop > v where v is a floating point number and the bucket is fully out of range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_float(10.0)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_float(10.5)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_float(15.0002)) shouldBe 0.0
  }

  test("n.prop >= v where v is a floating point number and the bucket is fully out of range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_float(10.0)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_float(10.5)) shouldBe 0.0
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_float(15.0002)) shouldBe 0.0
  }

  test("n.prop < v where v is an integer and the bucket is fully in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_int(9)) should not be 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_int(10)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_int(11)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_int(100)) shouldBe 0.1
  }

  test("n.prop <= v where v is an integer and the bucket is fully in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_int(9)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_int(10)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_int(11)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_int(100)) shouldBe 0.1
  }

  test("n.prop > v where v is an integer and the bucket is fully in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_int(5)) should not be 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_int(4)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_int(0)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_int(-14)) shouldBe 0.1
  }

  test("n.prop >= v where v is an integer and the bucket is fully in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_int(5)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_int(4)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_int(0)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_int(-14)) shouldBe 0.1
  }

  test("n.prop < v where v is a floating point number and the bucket is fully in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_float(10.0)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_float(10.0001)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLtV_float(80.3)) shouldBe 0.1
  }

  test("n.prop <= v where v is a floating point number and the bucket is fully in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_float(10.0)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_float(10.0001)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropLteV_float(80.3)) shouldBe 0.1
  }

  test("n.prop > v where v is a floating point number and the bucket is fully in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(5.0)
    ) shouldBe 0.1 +- defaultAllowedError
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_float(4.99999)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_float(2)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGtV_float(-2)) shouldBe 0.1
  }

  test("n.prop >= v where v is a floating point number and the bucket is fully in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_float(5.0)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_float(4.99999)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_float(2)) shouldBe 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(b, nPropGteV_float(-2)) shouldBe 0.1
  }

  test("n.prop < v where v is an integer and the bucket is partly in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(6)
    ) shouldBe 0.02 +- defaultAllowedError // (|{5}      | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(7)
    ) shouldBe 0.04 +- defaultAllowedError // (|{5,6}    | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(8)
    ) shouldBe 0.06 +- defaultAllowedError // (|{5,6,7}  | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(9)
    ) shouldBe 0.08 +- defaultAllowedError // (|{5,6,7,8}| / |{5,6,7,8,9}|) * 0.1
  }

  test("n.prop <= v where v is an integer and the bucket is partly in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(5)
    ) shouldBe 0.02 +- defaultAllowedError // (|{5}      | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(6)
    ) shouldBe 0.04 +- defaultAllowedError // (|{5,6}    | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(7)
    ) shouldBe 0.06 +- defaultAllowedError // (|{5,6,7}  | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(8)
    ) shouldBe 0.08 +- defaultAllowedError // (|{5,6,7,8}| / |{5,6,7,8,9}|) * 0.1
  }

  test("n.prop > v where v is an integer and the bucket is partly in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(8)
    ) shouldBe 0.02 +- defaultAllowedError // (|{9}      | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(7)
    ) shouldBe 0.04 +- defaultAllowedError // (|{8,9}    | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(6)
    ) shouldBe 0.06 +- defaultAllowedError // (|{7,8,9}  | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(5)
    ) shouldBe 0.08 +- defaultAllowedError // (|{6,7,8,9}| / |{5,6,7,8,9}|) * 0.1
  }

  test("n.prop >= v where v is an integer and the bucket is partly in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(9)
    ) shouldBe 0.02 +- defaultAllowedError // (|{9}      | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(8)
    ) shouldBe 0.04 +- defaultAllowedError // (|{8,9}    | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(7)
    ) shouldBe 0.06 +- defaultAllowedError // (|{7,8,9}  | / |{5,6,7,8,9}|) * 0.1
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(6)
    ) shouldBe 0.08 +- defaultAllowedError // (|{6,7,8,9}| / |{5,6,7,8,9}|) * 0.1
  }

  test("n.prop < v where v is a floating point number and the bucket is partly in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_float(5.001)
    ) shouldBe 0.00002 +- defaultAllowedError // (0.001/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_float(5.01)
    ) shouldBe 0.0002 +- defaultAllowedError // (0.01/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_float(5.1)
    ) shouldBe 0.002 +- defaultAllowedError // (0.1/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_float(6.0)
    ) shouldBe 0.02 +- defaultAllowedError // (1/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_float(7.5)
    ) shouldBe 0.05 +- defaultAllowedError // (2.5/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_float(9.0)
    ) shouldBe 0.08 +- defaultAllowedError // (4/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_float(9.9)
    ) shouldBe 0.098 +- defaultAllowedError // (4.9/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_float(9.99)
    ) shouldBe 0.0998 +- defaultAllowedError // (4.99/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_float(9.999)
    ) shouldBe 0.09998 +- defaultAllowedError // (4.99/5.0 * 0.1)
  }

  test("n.prop <= v where v is a floating point number and the bucket is partly in range") {
    val b = StandardBucket(5, 10, 0.1)

    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_float(5.001)
    ) shouldBe 0.00002 +- defaultAllowedError // (0.001/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_float(5.01)
    ) shouldBe 0.0002 +- defaultAllowedError // (0.01/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_float(5.1)
    ) shouldBe 0.002 +- defaultAllowedError // (0.1/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_float(6.0)
    ) shouldBe 0.02 +- defaultAllowedError // (1/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_float(7.5)
    ) shouldBe 0.05 +- defaultAllowedError // (2.5/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_float(9.0)
    ) shouldBe 0.08 +- defaultAllowedError // (4/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_float(9.9)
    ) shouldBe 0.098 +- defaultAllowedError // (4.9/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_float(9.99)
    ) shouldBe 0.0998 +- defaultAllowedError // (4.99/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_float(9.999)
    ) shouldBe 0.09998 +- defaultAllowedError // (4.99/5.0 * 0.1)
  }

  test("n.prop > v where v is a floating point number and the bucket is partly in range") {
    val b = StandardBucket(5, 10, 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(9.999)
    ) shouldBe 0.00002 +- defaultAllowedError // (0.001/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(9.99)
    ) shouldBe 0.0002 +- defaultAllowedError // (0.01/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(9.9)
    ) shouldBe 0.002 +- defaultAllowedError // (0.1/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(9.0)
    ) shouldBe 0.02 +- defaultAllowedError // (1/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(7.5)
    ) shouldBe 0.05 +- defaultAllowedError // (2.5/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(6)
    ) shouldBe 0.08 +- defaultAllowedError // (4/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(5.1)
    ) shouldBe 0.098 +- defaultAllowedError // (4.9/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(5.01)
    ) shouldBe 0.0998 +- defaultAllowedError // (4.99/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_float(5.001)
    ) shouldBe 0.09998 +- defaultAllowedError // (4.99/5.0 * 0.1)
  }

  test("n.prop >= v where v is a floating point number and the bucket is partly in range") {
    val b = StandardBucket(5, 10, 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_float(9.999)
    ) shouldBe 0.00002 +- defaultAllowedError // (0.001/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_float(9.99)
    ) shouldBe 0.0002 +- defaultAllowedError // (0.01/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_float(9.9)
    ) shouldBe 0.002 +- defaultAllowedError // (0.1/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_float(9.0)
    ) shouldBe 0.02 +- defaultAllowedError // (1/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_float(7.5)
    ) shouldBe 0.05 +- defaultAllowedError // (2.5/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_float(6)
    ) shouldBe 0.08 +- defaultAllowedError // (4/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_float(5.1)
    ) shouldBe 0.098 +- defaultAllowedError // (4.9/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_float(5.01)
    ) shouldBe 0.0998 +- defaultAllowedError // (4.99/5.0 * 0.1)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_float(5.001)
    ) shouldBe 0.09998 +- defaultAllowedError // (4.99/5.0 * 0.1)
  }

  test("Bucket covering a range with positive and negative numbers") {
    val b = StandardBucket(-3, 7, 0.2)

    // n.prop < v
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(-3)
    ) shouldBe 0.0 +- defaultAllowedError // 0/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(-2)
    ) shouldBe 0.02 +- defaultAllowedError // 1/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(0)
    ) shouldBe 0.06 +- defaultAllowedError // 3/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(2)
    ) shouldBe 0.1 +- defaultAllowedError // 5/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(7)
    ) shouldBe 0.2 +- defaultAllowedError // 10/10 * 0.2

    // n.prop <= v
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(-4)
    ) shouldBe 0.0 +- defaultAllowedError // 0/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(-3)
    ) shouldBe 0.02 +- defaultAllowedError // 1/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(-1)
    ) shouldBe 0.06 +- defaultAllowedError // 3/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(1)
    ) shouldBe 0.1 +- defaultAllowedError // 5/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(7)
    ) shouldBe 0.2 +- defaultAllowedError // 10/10 * 0.2

    // n.prop > v
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(6)
    ) shouldBe 0.0 +- defaultAllowedError // 0/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(5)
    ) shouldBe 0.02 +- defaultAllowedError // 1/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(3)
    ) shouldBe 0.06 +- defaultAllowedError // 3/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(1)
    ) shouldBe 0.1 +- defaultAllowedError // 5/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(-2)
    ) shouldBe 0.16 +- defaultAllowedError // 8/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(-4)
    ) shouldBe 0.2 +- defaultAllowedError // 10/10 * 0.2

    // n.prop >= v
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(7)
    ) shouldBe 0.0 +- defaultAllowedError // 0/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(6)
    ) shouldBe 0.02 +- defaultAllowedError // 1/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(4)
    ) shouldBe 0.06 +- defaultAllowedError // 3/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(2)
    ) shouldBe 0.1 +- defaultAllowedError // 5/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(-1)
    ) shouldBe 0.16 +- defaultAllowedError // 8/10 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(-3)
    ) shouldBe 0.2 +- defaultAllowedError // 10/10 * 0.2
  }

  test("Bucket covering only negative numbers") {
    val b = StandardBucket(-3, -1, 0.2)

    // n.prop < v
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(-3)
    ) shouldBe 0.0 +- defaultAllowedError // 0/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(-2)
    ) shouldBe 0.1 +- defaultAllowedError // 1/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(-1)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(0)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLtV_int(2)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2

    // n.prop <= v
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(-4)
    ) shouldBe 0.0 +- defaultAllowedError // 0/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(-3)
    ) shouldBe 0.1 +- defaultAllowedError // 1/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(-2)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(-1)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(0)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropLteV_int(2)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2

    // n.prop > v
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(-2)
    ) shouldBe 0.0 +- defaultAllowedError // 0/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(-3)
    ) shouldBe 0.1 +- defaultAllowedError // 1/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGtV_int(-4)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2

    // n.prop >= v
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(-1)
    ) shouldBe 0.0 +- defaultAllowedError // 0/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(-2)
    ) shouldBe 0.1 +- defaultAllowedError // 1/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(-3)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      nPropGteV_int(-4)
    ) shouldBe 0.2 +- defaultAllowedError // 2/2 * 0.2
  }

  test("n.prop > 20 and n.prop < 25") {
    // {21, 22, 23, 24} -> 4
    val rangePredicates = nPropGtLt_int(20, 25)

    val b = StandardBucket(10, 30, 0.5)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      rangePredicates
    ) shouldBe 0.1 +- defaultAllowedError // 4/20 * 0.5
  }

  test("n.prop > 20.0 and n.prop < 25.0") {
    // (20, 25) -> ~5
    val rangePredicates = nPropGtLt_float(20.0, 25.0)

    val b = StandardBucket(10, 30, 0.5)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      rangePredicates
    ) shouldBe 0.125 +- defaultAllowedError // 5/20 * 0.5
  }

  test("n.prop > 20 and n.prop < 25.0") {
    // Mixing integer and floating point values is not recommended!

    // Integer reasoning: {21, 22, 23, 24} => 4
    // Floating point reasoning: (20, 25) => 5
    // Mixed reasoning: [21, 25) => 4

    // The actual calculation:
    // n.prop > 20   satisfies 45% (and therefore prunes 55%) of the buckets: {21, ..., 29}
    // n.prop < 25.0 satisfies 75% (and therefore pruned 25%) of the buckets: [10, 25)
    // This means that both predicates satisfy 100-55-25 = 20% of the bucket (4 out of 20 elements)
    val rangePredicates = nPropGtLt_int_float(20, 25.0)

    val b = StandardBucket(10, 30, 0.5)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      rangePredicates
    ) shouldBe 0.1 +- defaultAllowedError // 0.2 * 0.5
  }

  test("n.prop > 20.0 and n.prop < 25") {
    // Mixing integer and floating point values is not recommended!
    // Integer reasoning: {21, 22, 23, 24} => 4
    // Floating point reasoning: (20, 25) => ~5
    // Mixed reasoning: (20, 24] => 4

    // The actual calculation:
    // n.prop > 20.0 satisfies ~50% (and therefore prunes ~50%) of the bucket: (20, 30)
    // n.prop < 25   satisfies  75% (and therefore prunes 25%) of the bucket: {10, ..., 24} out of {10, ..., 29}
    // This means that both predicates satisfy 100-~50-25=~25% of the bucket -> 5 elements out of 20

    // The difference comes form the fact that n.prop > 20.0 and n.prop > 20 are not the same.
    // Assume that the exclusive upperbound of the bucket is 21, then
    // - n.prop > 20 does not contain any value in the bucket                                              : 0 elements
    // - n.prop > 20.0 matches all values between 20.00000...01 and 20.99999...99, which is approximately 1: 1 element

    // This is desired behaviour. However, when mixing integer and floating point number, it becomes complicated.
    val rangePredicates = nPropGtLt_float_int(20.0, 25)

    val b = StandardBucket(10, 30, 0.5)
    EstimateSelectivityUsingHistogram.estimateBucketSelectivity(
      b,
      rangePredicates
    ) shouldBe 0.125 +- defaultAllowedError // 0.25 * 0.5
  }
}
