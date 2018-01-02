/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticError, SemanticState}

class HexIntegerLiteralTest extends CypherFunSuite {
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
    val result = literal.semanticCheck(SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, DummyPosition(4))))
  }
}
