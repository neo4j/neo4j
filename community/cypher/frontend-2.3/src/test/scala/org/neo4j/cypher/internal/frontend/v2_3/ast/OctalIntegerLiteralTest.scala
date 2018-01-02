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
    val result = literal.semanticCheck(SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, DummyPosition(4))))
  }
}
