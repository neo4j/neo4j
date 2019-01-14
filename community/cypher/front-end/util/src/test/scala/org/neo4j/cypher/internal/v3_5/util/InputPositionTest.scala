/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.util

import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class InputPositionTest extends CypherFunSuite {

  test("should bump the input position") {
    val pos = InputPosition(2, 1, 1)

    val bumped = pos.bumped()

    bumped.offset should equal(pos.offset + 1)
    bumped.column should equal(pos.column)
    bumped.line should equal(pos.line)

    pos should not equal bumped
    pos should equal(InputPosition(2, 1, 1))
    bumped should equal(InputPosition(2, 1, 1).bumped())
  }

  test("should print offset") {
    InputPosition(2, 1, 1).toOffsetString should equal("2")
    InputPosition(2, 1, 1).bumped().toOffsetString should equal("3")
  }
}
