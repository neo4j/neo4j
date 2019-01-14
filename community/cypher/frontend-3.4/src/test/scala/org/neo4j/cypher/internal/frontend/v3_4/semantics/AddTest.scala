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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.util.v3_4.DummyPosition
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions.Add

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
    testValidTypes(CTDuration, CTDuration)(CTDuration)
    testValidTypes(CTDate, CTDuration)(CTDate)
    testValidTypes(CTDuration, CTDate)(CTDate)
    testValidTypes(CTTime, CTDuration)(CTTime)
    testValidTypes(CTDuration, CTTime)(CTTime)
    testValidTypes(CTLocalTime, CTDuration)(CTLocalTime)
    testValidTypes(CTDuration, CTLocalTime)(CTLocalTime)
    testValidTypes(CTDateTime, CTDuration)(CTDateTime)
    testValidTypes(CTDuration, CTDateTime)(CTDateTime)
    testValidTypes(CTLocalDateTime, CTDuration)(CTLocalDateTime)
    testValidTypes(CTDuration, CTLocalDateTime)(CTLocalDateTime)

    testValidTypes(CTList(CTNode), CTList(CTNode))(CTList(CTNode))
    testValidTypes(CTList(CTFloat), CTList(CTFloat))(CTList(CTFloat))

    testValidTypes(CTList(CTNode), CTNode)(CTList(CTNode))
    testValidTypes(CTList(CTFloat), CTFloat)(CTList(CTFloat))

    testValidTypes(CTNode, CTList(CTNode))(CTList(CTNode))
    testValidTypes(CTFloat, CTList(CTFloat))(CTList(CTFloat))

    testValidTypes(CTList(CTAny), CTList(CTAny))(CTList(CTAny))
  }

  test("should handle covariant types") {
    testValidTypes(CTString.covariant, CTString.covariant)(CTString)
    testValidTypes(CTString.covariant, CTInteger)(CTString)
    testValidTypes(CTString, CTInteger.covariant)(CTString)
    testValidTypes(CTInteger.covariant, CTInteger.covariant)(CTInteger)
    testValidTypes(CTInteger.covariant, CTFloat)(CTFloat)
    testValidTypes(CTInteger, CTFloat.covariant)(CTFloat)
    testValidTypes(CTFloat.covariant, CTFloat.covariant)(CTFloat)

    testValidTypes(CTList(CTFloat).covariant, CTList(CTFloat).covariant)(CTList(CTFloat))

    testValidTypes(CTList(CTNode).covariant, CTNode)(CTList(CTNode))
    testValidTypes(CTList(CTNode), CTNode.covariant)(CTList(CTNode))

    testValidTypes(CTList(CTAny).covariant, CTList(CTAny).covariant)(CTList(CTAny).covariant)
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
      "Type mismatch: expected Float, Integer, String or List<T> but was Boolean"
    )
    testInvalidApplication(CTDuration, CTBoolean)(
      "Type mismatch: expected Duration, Date, Time, LocalTime, LocalDateTime, DateTime or List<T> but was Boolean"
    )
  }

  test("should concatenate different typed lists") {
    testValidTypes(CTList(CTInteger), CTList(CTString))(CTList(CTAny))
  }

  test("should concatenate vector element of other type after list") {
    testValidTypes(CTInteger, CTList(CTString))(CTList(CTAny))
  }

  test("should concatenate vector element of other type before list") {
    testValidTypes(CTList(CTInteger), CTString)(CTList(CTAny))
  }

  test("should concatenate same typed lists") {
    testValidTypes(CTList(CTInteger), CTList(CTInteger))(CTList(CTInteger))
  }

  test("should concatenate nested lists") {
    testValidTypes(CTList(CTList(CTInteger)), CTList(CTList(CTInteger)))(CTList(CTList(CTInteger)))
    testValidTypes(CTList(CTList(CTInteger)), CTList(CTInteger))(CTList(CTAny))
    testValidTypes(CTList(CTList(CTInteger)), CTInteger)(CTList(CTAny))
  }

  test("should work with ORed types") {
    testValidTypes(CTInteger | CTList(CTString), CTList(CTString) | CTInteger)(CTList(CTAny) | CTList(CTString) | CTInteger)
    testValidTypes(CTInteger | CTList(CTInteger), CTString)(CTString | CTList(CTAny))
    testValidTypes(CTInteger | CTList(CTInteger), CTBoolean)(CTList(CTAny))
  }
}
