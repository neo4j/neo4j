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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.apa.v3_4.DummyPosition
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.ast.SignedOctalIntegerLiteral

class OctalIntegerLiteralTest extends CypherFunSuite {
  test("correctly parses octal numbers") {
    assert(SignedOctalIntegerLiteral("022")(DummyPosition(0)).value === 0x12)
    assert(SignedOctalIntegerLiteral("00")(DummyPosition(0)).value === 0x0)
    assert(SignedOctalIntegerLiteral("0734")(DummyPosition(0)).value === 0x1dc)
    assert(SignedOctalIntegerLiteral("0034")(DummyPosition(0)).value === 0x1c)
  }

  test("throws error for invalid octal numbers") {
    assertSemanticError("0393", "invalid literal number")
    assertSemanticError("03f4", "invalid literal number")
    assertSemanticError("-0934", "invalid literal number")
  }

  test("throws error for too large octal numbers") {
    assertSemanticError("077777777777777777777777777777", "integer is too large")
  }

  private def assertSemanticError(stringValue: String, errorMessage: String) {
    val literal = SignedOctalIntegerLiteral(stringValue)(DummyPosition(4))
    val result = SemanticAnalysis.semanticCheck(SemanticContext.Simple, literal)(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, DummyPosition(4))))
  }
}
