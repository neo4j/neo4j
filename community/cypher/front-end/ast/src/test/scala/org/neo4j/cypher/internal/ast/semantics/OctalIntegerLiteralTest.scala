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

import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.util.DummyPosition

class OctalIntegerLiteralTest extends SemanticFunSuite {

  // old syntax
  test("correctly parses old syntax ocatal numbers") {
    assert(SignedOctalIntegerLiteral("022")(DummyPosition(0)).value === java.lang.Long.decode("022"))
    assert(SignedOctalIntegerLiteral("00")(DummyPosition(0)).value === java.lang.Long.decode("00"))
    assert(SignedOctalIntegerLiteral("0777")(DummyPosition(0)).value === java.lang.Long.decode("0777"))
    assert(SignedOctalIntegerLiteral("-0123")(DummyPosition(0)).value === java.lang.Long.decode("-0123"))
  }

  test("correctly parses ocatal numbers") {
    assert(SignedOctalIntegerLiteral("0o22")(DummyPosition(0)).value === java.lang.Long.decode("022"))
    assert(SignedOctalIntegerLiteral("0o0")(DummyPosition(0)).value === java.lang.Long.decode("00"))
    assert(SignedOctalIntegerLiteral("0o777")(DummyPosition(0)).value === java.lang.Long.decode("0777"))
    assert(SignedOctalIntegerLiteral("-0o123")(DummyPosition(0)).value === java.lang.Long.decode("-0123"))

  }

  // old syntax
  test("throws error for invalid old syntax octal numbers") {
    assertSemanticError("012a3", "invalid literal number")
    assertSemanticError("01911", "invalid literal number")
    assertSemanticError("0O22", "invalid literal number")
    assertSemanticError("0O0", "invalid literal number")
    assertSemanticError("0O777", "invalid literal number")
    assertSemanticError("-0O123", "invalid literal number")
  }

  test("throws error for invalid octal numbers") {
    assertSemanticError("0o12a3", "invalid literal number")
    assertSemanticError("0o1911", "invalid literal number")
  }

  // old syntax
  test("throws error for too large old syntax octal numbers") {
    assertSemanticError("010000000000000000000000", "integer is too large")
  }

  test("throws error for too large octal numbers") {
    assertSemanticError("0o10000000000000000000000", "integer is too large")
  }

  // old syntax
  test("correctly parse old syntax octal Long.MIN_VALUE") {
    assert(SignedOctalIntegerLiteral("-01000000000000000000000")(DummyPosition(0)).value === Long.MinValue)
  }

  test("correctly parse octal Long.MIN_VALUE") {
    assert(SignedOctalIntegerLiteral("-0o1000000000000000000000")(DummyPosition(0)).value === Long.MinValue)
  }

  private def assertSemanticError(stringValue: String, errorMessage: String): Unit = {
    val literal = SignedOctalIntegerLiteral(stringValue)(DummyPosition(4))
    val result = SemanticExpressionCheck.check(SemanticContext.Simple, literal)(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, DummyPosition(4))))
  }
}
