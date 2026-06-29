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

import org.scalatest.exceptions.TestFailedException

class CompareAsPrettyStringsTest extends CypherFunSuite {

  private case class Person(name: String, age: Int)

  test("should fail when values are not equal") {
    val ex = intercept[TestFailedException] {
      Person("Alice", 20) compareAsPrettyStrings Person("Bob", 30)
    }
    ex.getMessage shouldEqual """Person(name = "Alice", age = 20) did not equal Person(name = "Bob", age = 30)"""
  }

  test("should fail when values are equal") {
    val ex = intercept[TestFailedException] {
      val p = Person("Alice", 20)
      p compareAsPrettyStrings p
    }
    ex.getMessage shouldEqual "compareAsPrettyStrings is only for debugging and should not be committed"
  }
}
