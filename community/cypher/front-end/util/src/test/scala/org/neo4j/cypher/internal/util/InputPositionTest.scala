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

class InputPositionTest extends CypherFunSuite {

  test("Adjust offset") {

    // CYPHER runtime=slotted RETURN 1
    //                               ^
    InputPosition(7, 1, 8).withOffset(Some(InputPosition(23, 1, 24))) shouldBe InputPosition(30, 1, 31)

    // CYPHER \nruntime=slotted RETURN 1
    //                                 ^
    InputPosition(7, 1, 8).withOffset(Some(InputPosition(24, 2, 15))) shouldBe InputPosition(31, 2, 22)

    // CYPHER runtime=slotted \nRETURN 1
    //                                 ^
    InputPosition(7, 1, 8).withOffset(Some(InputPosition(24, 2, 1))) shouldBe InputPosition(31, 2, 8)
  }
}
