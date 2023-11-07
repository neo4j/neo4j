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

import org.neo4j.cypher.internal.util.PredicateCostTest.arbitraryPredicateCost
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
}

object PredicateCostTest {
  implicit val arbitraryPredicateCost: Arbitrary[PredicateCost] = Arbitrary(genPredicateCost)

  private def genPredicateCost: Gen[PredicateCost] =
    for {
      costPerRow <- genCostPerRow
      selectivity <- genSelectivity
    } yield PredicateCost(costPerRow, selectivity)

  private def genCostPerRow: Gen[CostPerRow] =
    for {
      size <- Gen.size
      cost <- Gen.chooseNum[Double](0, size, 0.000001)
    } yield CostPerRow(cost)

  private def genSelectivity: Gen[Selectivity] =
    for {
      factor <- Gen.chooseNum[Double](0, 1, 0.000001)
    } yield Selectivity(factor)
}
