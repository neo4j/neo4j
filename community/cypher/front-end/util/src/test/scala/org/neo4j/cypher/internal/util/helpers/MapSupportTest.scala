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
package org.neo4j.cypher.internal.util.helpers

import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class MapSupportTest extends CypherFunSuite {

  test("fuse with set difference different keys") {
    val result = Map(1 -> Set("a", "b"), 2 -> Set("c")).fuse(Map(1 -> Set("b", "c"), 3 -> Set("d")))(_ -- _)
    result should equal(Map(1 -> Set("a"), 2 -> Set("c"), 3 -> Set("d")))
  }

  test("fuseLeft with set difference ab bc") {
    val result = Map(1 -> Set("a", "b")).fuseLeft(Map(1 -> Set("b", "c")))(_ -- _)
    result should equal(Map(1 -> Set("a")))
  }

  test("fuseLeft with set difference bc ab") {
    val result = Map(1 -> Set("b", "c")).fuseLeft(Map(1 -> Set("a", "b")))(_ -- _)
    result should equal(Map(1 -> Set("c")))
  }

  test("fuseLeft with set difference different keys") {
    val result = Map(1 -> Set("a", "b"), 2 -> Set("c")).fuseLeft(Map(1 -> Set("b", "c"), 3 -> Set("d")))(_ -- _)
    result should equal(Map(1 -> Set("a"), 2 -> Set("c")))
  }

  test("fuseLeft with LHS empty map") {
    val result = Map.empty[Int, Set[String]].fuseLeft(Map(1 -> Set("a")))(_ -- _)
    result should equal(Map())
  }

  test("fuseLeft with RHS empty map") {
    val result = Map(1 -> Set("a")).fuseLeft(Map.empty)(_ -- _)
    result should equal(Map(1 -> Set("a")))
  }

  test("fuseLeft with remaining empty set as value") {
    val result = Map(1 -> Set("a")).fuseLeft(Map(1 -> Set("a")))(_ -- _)
    result should equal(Map(1 -> Set()))
  }
}
