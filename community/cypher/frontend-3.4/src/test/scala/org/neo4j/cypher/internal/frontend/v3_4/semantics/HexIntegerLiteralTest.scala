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
import org.neo4j.cypher.internal.v3_4.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.v3_4.expressions.SignedHexIntegerLiteral

class HexIntegerLiteralTest extends SemanticFunSuite {
  test("correctly parses hexadecimal numbers") {
    assert(SignedHexIntegerLiteral("0x22")(DummyPosition(0)).value === 0x22)
    assert(SignedHexIntegerLiteral("0x0")(DummyPosition(0)).value === 0)
    assert(SignedHexIntegerLiteral("0xffFF")(DummyPosition(0)).value === 0xffff)
    assert(SignedHexIntegerLiteral("-0x9abc")(DummyPosition(0)).value === -0x9abc)
  }

  test("throws error for invalid hexadecimal numbers") {
    assertSemanticError("0x12g3", "invalid literal number")
    assertSemanticError("0x", "invalid literal number")
    assertSemanticError("0x33Y23", "invalid literal number")
    assertSemanticError("-0x12g3", "invalid literal number")
  }

  test("throws error for too large hexadecimal numbers") {
    assertSemanticError("0xfffffffffffffffff", "integer is too large")
  }

  private def assertSemanticError(stringValue: String, errorMessage: String) {
    val literal = SignedHexIntegerLiteral(stringValue)(DummyPosition(4))
    val result = SemanticExpressionCheck.check(SemanticContext.Simple, literal)(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, DummyPosition(4))))
  }
}
