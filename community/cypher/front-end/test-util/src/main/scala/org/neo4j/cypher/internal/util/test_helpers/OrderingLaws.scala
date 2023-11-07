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
package org.neo4j.cypher.internal.util.test_helpers

import org.scalacheck.Arbitrary

abstract class OrderingLaws[A : Ordering : Arbitrary] extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {

  private val ordering: Ordering[A] = Ordering[A]

  test("totality") {
    forAll { (x: A, y: A) =>
      (ordering.lteq(x, y) || ordering.lteq(y, x)) shouldBe true
    }
  }

  test("transitivity") {
    forAll { (x: A, y: A, z: A) =>
      whenever(ordering.lteq(x, y) && ordering.lteq(y, z)) {
        ordering.lteq(x, z) shouldBe true
      }
    }
  }

  test("reflexivity") {
    forAll { x: A =>
      ordering.lteq(x, x) shouldBe true
    }
  }

  test("antisymmetry") {
    forAll { (x: A, y: A) =>
      // Using material implication instead of `whenever` as the latter would reject too many generated values.
      // This is equivalent to: x <= y && y <= x -> x == y
      (!(ordering.lteq(x, y) && ordering.lteq(y, x)) || ordering.equiv(x, y)) shouldBe true
    }
  }
}
