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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class FunctionInvocationTest extends CypherFunSuite {

  private val pos = InputPosition.NONE

  private def invocation(name: String, isShadowed: Boolean = false): FunctionInvocation =
    FunctionInvocation(
      FunctionName(name)(pos),
      distinct = false,
      IndexedSeq.empty,
      isShadowed = isShadowed
    )(pos)

  test("isBuiltIn is true for a genuine compiler built-in function") {
    invocation("point").isBuiltIn shouldBe true
  }

  test("isBuiltIn is false for a shadowed compiler built-in function") {
    invocation("point", isShadowed = true).isBuiltIn shouldBe false
  }

  test("isBuiltIn is false for a function name that is not a compiler built-in") {
    invocation("date").isBuiltIn shouldBe false
  }

  test("isBuiltIn is false for a user-defined function name") {
    invocation("my.udf").isBuiltIn shouldBe false
  }
}
