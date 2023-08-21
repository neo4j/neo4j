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

import org.neo4j.cypher.internal.util.ZippableUtil.Zippable
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ZippableTest extends CypherFunSuite {

  test("should behave like zip/zipAll depending on which list is longer") {
    for {
      leftLength <- 1 to 10
      left <- (1 to 10).combinations(leftLength)
      rightLength <- 1 to 10
      right <- (1 to 10).combinations(rightLength)
    } {
      if (left.length <= right.length) {
        left.zipLeft(right, 0) should equal(left.zip(right))
        left.zipRight(right, 0) should equal(left.zipAll(right, 0, 1))
      } else {
        left.zipLeft(right, 0) should equal(left.zipAll(right, 1, 0))
        left.zipRight(right, 0) should equal(left.zip(right))
      }
    }
  }
}
