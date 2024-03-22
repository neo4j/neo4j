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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.expressions.Concatenate
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime

class ConcatenationTest extends InfixExpressionTestBase(Concatenate(_, _)(DummyPosition(0))) {

  // Infix specializations:
  // "a" || "b" => "ab"
  // [a] || [b] => [a, b]

  test("shouldHandleAllSpecializations") {
    testValidTypes(CTString, CTString)(CTString)
    testValidTypes(CTList(CTAny), CTList(CTAny))(CTList(CTAny))
  }

  test("should handle covariant types") {
    testValidTypes(CTString.covariant, CTString.covariant)(CTString)
    testValidTypes(CTList(CTAny).covariant, CTList(CTAny).covariant)(CTList(CTAny).covariant)
  }

  test("shouldHandleCombinedSpecializations") {
    testValidTypes(CTFloat | CTString, CTString)(CTString)
  }

  test("shouldFailTypeCheckForIncompatibleArguments") {
    testInvalidApplication(CTString, CTBoolean)(
      "Type mismatch: expected String but was Boolean"
    )
    testInvalidApplication(CTDuration, CTBoolean)(
      "Type mismatch: expected String or List<T> but was Duration"
    )
    testInvalidApplication(CTList(CTBoolean), CTBoolean)(
      "Type mismatch: expected List<T> but was Boolean"
    )
    testInvalidApplication(CTBoolean, CTList(CTBoolean))(
      "Type mismatch: expected String or List<T> but was Boolean"
    )
    testInvalidApplication(CTString, CTFloat)(
      "Type mismatch: expected String but was Float"
    )
    testInvalidApplication(CTInteger, CTString)(
      "Type mismatch: expected String or List<T> but was Integer"
    )
    testInvalidApplication(CTInteger, CTInteger)(
      "Type mismatch: expected String or List<T> but was Integer"
    )
    testInvalidApplication(CTInteger, CTFloat)(
      "Type mismatch: expected String or List<T> but was Integer"
    )
    testInvalidApplication(CTFloat, CTString)(
      "Type mismatch: expected String or List<T> but was Float"
    )
    testInvalidApplication(CTFloat, CTInteger)(
      "Type mismatch: expected String or List<T> but was Float"
    )
    testInvalidApplication(CTFloat, CTFloat)(
      "Type mismatch: expected String or List<T> but was Float"
    )
    testInvalidApplication(CTDuration, CTDuration)(
      "Type mismatch: expected String or List<T> but was Duration"
    )
    testInvalidApplication(CTDate, CTDuration)(
      "Type mismatch: expected String or List<T> but was Date"
    )
    testInvalidApplication(CTDuration, CTDate)(
      "Type mismatch: expected String or List<T> but was Duration"
    )
    testInvalidApplication(CTTime, CTDuration)(
      "Type mismatch: expected String or List<T> but was Time"
    )
    testInvalidApplication(CTDuration, CTTime)(
      "Type mismatch: expected String or List<T> but was Duration"
    )
    testInvalidApplication(CTLocalTime, CTDuration)(
      "Type mismatch: expected String or List<T> but was LocalTime"
    )
    testInvalidApplication(CTDuration, CTLocalTime)(
      "Type mismatch: expected String or List<T> but was Duration"
    )
    testInvalidApplication(CTDateTime, CTDuration)(
      "Type mismatch: expected String or List<T> but was DateTime"
    )
    testInvalidApplication(CTDuration, CTDateTime)(
      "Type mismatch: expected String or List<T> but was Duration"
    )
    testInvalidApplication(CTLocalDateTime, CTDuration)(
      "Type mismatch: expected String or List<T> but was LocalDateTime"
    )
    testInvalidApplication(CTDuration, CTLocalDateTime)(
      "Type mismatch: expected String or List<T> but was Duration"
    )
  }

  test("should concatenate different typed lists") {
    testValidTypes(CTList(CTInteger), CTList(CTString))(CTList(CTAny))
  }

  test("should concatenate lists with empty lists") {
    val expected = CTList(CTAny) | CTList(CTInteger) | CTList(CTFloat) | CTList(CTNumber)
    testValidTypes(CTList(CTInteger), CTList(CTAny).covariant)(expected)
    testValidTypes(CTList(CTAny).covariant, CTList(CTInteger))(expected)
  }

  test("should concatenate same typed lists") {
    testValidTypes(CTList(CTInteger), CTList(CTInteger))(CTList(CTInteger))
  }

  test("should concatenate nested lists") {
    testValidTypes(CTList(CTList(CTInteger)), CTList(CTList(CTInteger)))(CTList(CTList(CTInteger)))
    testValidTypes(CTList(CTList(CTInteger)), CTList(CTInteger))(CTList(CTAny))
  }
}
