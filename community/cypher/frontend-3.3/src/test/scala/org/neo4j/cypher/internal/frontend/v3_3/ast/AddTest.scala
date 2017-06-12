/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_3.symbols._

class AddTest extends InfixExpressionTestBase(Add(_, _)(DummyPosition(0))) {

  // Infix specializations:
  // "a" + "b" => "ab"
  // "a" + 1 => "a1"
  // "a" + 1.1 => "a1.1"
  // 1 + "b" => "1b"
  // 1 + 1 => 2
  // 1 + 1.1 => 2.1
  // 1.1 + "b" => "1.1b"
  // 1.1 + 1 => 2.1
  // 1.1 + 1.1 => 2.2
  // [a] + [b] => [a, b]
  // [a] + b => [a, b]
  // a + [b] => [a, b]

  test("shouldHandleAllSpecializations") {
    testValidTypes(CTString, CTString)(CTString)
    testValidTypes(CTString, CTInteger)(CTString)
    testValidTypes(CTString, CTFloat)(CTString)
    testValidTypes(CTInteger, CTString)(CTString)
    testValidTypes(CTInteger, CTInteger)(CTInteger)
    testValidTypes(CTInteger, CTFloat)(CTFloat)
    testValidTypes(CTFloat, CTString)(CTString)
    testValidTypes(CTFloat, CTInteger)(CTFloat)
    testValidTypes(CTFloat, CTFloat)(CTFloat)

    testValidTypes(CTList(CTNode), CTList(CTNode))(CTList(CTNode))
    testValidTypes(CTList(CTFloat), CTList(CTFloat))(CTList(CTFloat))

    testValidTypes(CTList(CTNode), CTNode)(CTList(CTNode))
    testValidTypes(CTList(CTFloat), CTFloat)(CTList(CTFloat))

    testValidTypes(CTNode, CTList(CTNode))(CTList(CTNode))
    testValidTypes(CTFloat, CTList(CTFloat))(CTList(CTFloat))
  }

  test("shouldHandleCombinedSpecializations") {
    testValidTypes(CTFloat | CTString, CTInteger)(CTFloat | CTString)
    testValidTypes(CTFloat | CTList(CTFloat), CTFloat)(CTFloat | CTList(CTFloat))
    testValidTypes(CTFloat, CTFloat | CTList(CTFloat))(CTFloat | CTList(CTFloat))
  }

  test("shouldHandleCoercions") {
    testValidTypes(CTList(CTFloat), CTInteger)(CTList(CTFloat))
    testValidTypes(CTFloat | CTList(CTFloat), CTInteger)(CTFloat | CTList(CTFloat))
  }

  test("shouldFailTypeCheckForIncompatibleArguments") {
    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Float, Integer, String or List<Integer> but was Boolean"
    )
    testInvalidApplication(CTList(CTInteger), CTString)(
      "Type mismatch: expected Integer, List<Integer> or List<List<Integer>> but was String"
    )
    testInvalidApplication(CTList(CTInteger), CTList(CTString))(
      "Type mismatch: expected Integer, List<Integer> or List<List<Integer>> but was List<String>"
    )
  }
}
