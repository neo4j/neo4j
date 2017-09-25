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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.{DummyPosition, SemanticError, SemanticState}

class UnsignedDecimalIntegerLiteralTest extends CypherFunSuite {
  test("correctly parses decimal numbers") {
    assert(UnsignedDecimalIntegerLiteral("22")(DummyPosition(0)).value === 22)
    assert(UnsignedDecimalIntegerLiteral("0")(DummyPosition(0)).value === 0)
  }

  test("throws error for invalid decimal numbers") {
    assertSemanticError("12g3", "invalid literal number")
    assertSemanticError("923_23", "invalid literal number")
  }

  test("throws error for too large decimal numbers") {
    assertSemanticError("999999999999999999999999999", "integer is too large")
  }

  private def assertSemanticError(stringValue: String, errorMessage: String) {
    val literal = UnsignedDecimalIntegerLiteral(stringValue)(DummyPosition(4))
    val result = literal.semanticCheck(SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, DummyPosition(4))))
  }
}
