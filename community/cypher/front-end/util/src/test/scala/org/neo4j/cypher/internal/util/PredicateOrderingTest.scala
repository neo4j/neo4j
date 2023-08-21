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

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PredicateOrderingTest extends CypherFunSuite {

  test("should not break transitivity due to floating point precision") {
    val a = (CostPerRow(4.0), Selectivity.ONE)
    val b = (CostPerRow(8.0), Selectivity.ONE)
    val c = (CostPerRow(8.0), Selectivity(0.9999999999999998))

    // a = b = c
    PredicateOrdering.equiv(a, b) shouldBe true
    PredicateOrdering.equiv(b, c) shouldBe true
    PredicateOrdering.equiv(a, c) shouldBe true
  }
}
