/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.PredicateCost.Tolerance
import org.neo4j.cypher.internal.util.PredicateCostTest.arbitraryPredicateCost
import org.neo4j.cypher.internal.util.PredicateCostTest.arbitraryTolerance
import org.neo4j.cypher.internal.util.test_helpers.OrderingLaws
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

class PredicateCostTest extends OrderingLaws[PredicateCost] {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  test("should not break transitivity due to floating point precision") {
    val a = PredicateCost(CostPerRow(4.0), Selectivity.ONE)
    val b = PredicateCost(CostPerRow(8.0), Selectivity(0.999999))
    val c = PredicateCost(CostPerRow(8.0), Selectivity(0.999998))

    // a > b > c
    (a > b) shouldBe true
    (b > c) shouldBe true
    (a > c) shouldBe true
  }

  test("two predicates with the same cost should be equal with a tolerance of 0") {
    val a = PredicateCost(CostPerRow(2.0), Selectivity(0.5))
    val b = PredicateCost(CostPerRow(2.4), Selectivity(0.4))

    a shouldEqual b
    a.equalsWithTolerance(b, Tolerance.zero) shouldBe true
  }

  test("any two predicates with the same cost should be equal with any tolerance") {
    forAll { (a: PredicateCost, b: PredicateCost, tolerance: Tolerance) =>
      (!(a == b) || a.equalsWithTolerance(b, tolerance)) shouldBe true
    }
  }

  test("the no-op predicate should be equal to a free predicate with a tolerance of 0") {
    val a = PredicateCost(CostPerRow(0.0), Selectivity.ONE)
    val b = PredicateCost(CostPerRow(0.0), Selectivity(0.5))

    (a == b) shouldBe false
    a.equalsWithTolerance(b, Tolerance.zero) shouldBe true
  }

  test("two predicates with almost the same cost should be equal with a big enough tolerance") {
    val a = PredicateCost(CostPerRow(2.0), Selectivity(0.52))
    val b = PredicateCost(CostPerRow(2.4), Selectivity(0.4))

    (a == b) shouldBe false
    a.equalsWithTolerance(b, Tolerance(0.01)) shouldBe false
    a.equalsWithTolerance(b, Tolerance(0.1)) shouldBe true
  }
}

object PredicateCostTest {
  implicit val arbitraryPredicateCost: Arbitrary[PredicateCost] = Arbitrary(genPredicateCost)
  implicit val arbitraryTolerance: Arbitrary[Tolerance] = Arbitrary(genTolerance)

  private def genPredicateCost: Gen[PredicateCost] =
    for {
      costPerRow <- genCostPerRow
      selectivity <- genSelectivity
    } yield PredicateCost(costPerRow, selectivity)

  private def genCostPerRow: Gen[CostPerRow] =
    genSizedNonNegativeDouble.map(CostPerRow.apply)

  private def genSelectivity: Gen[Selectivity] =
    genNonNegativeDouble(1.0).map(Selectivity.apply)

  private def genTolerance: Gen[Tolerance] =
    genSizedNonNegativeDouble.map(Tolerance.apply)

  private def genSizedNonNegativeDouble: Gen[Double] =
    Gen.sized(size => genNonNegativeDouble(size.toDouble))

  private def genNonNegativeDouble(max: Double): Gen[Double] =
    Gen.chooseNum[Double](0, max, Tolerance.default.value)
}
