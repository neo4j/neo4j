/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

class InputPositionTest extends CypherFunSuite {

  test("should create unique input positions") {
    val pos = InputPosition(2, 1, 1)

    val posCopy = pos.newUniquePos()

    posCopy.offset should equal(pos.offset)
    posCopy.column should equal(pos.column)
    posCopy.line should equal(pos.line)

    pos should not equal posCopy
  }

  test("copy should create unique input positions") {
    val pos = InputPosition(2, 1, 1)

    val posCopy = pos.copy(line = 1) // Same line

    posCopy.offset should equal(pos.offset)
    posCopy.column should equal(pos.column)
    posCopy.line should equal(pos.line)

    pos should not equal posCopy
  }

  test("should print offset") {
    InputPosition(2, 1, 1).toUniqueOffsetString should startWith("2")
    InputPosition(2, 1, 1).newUniquePos().toUniqueOffsetString should startWith("2")
  }
}
