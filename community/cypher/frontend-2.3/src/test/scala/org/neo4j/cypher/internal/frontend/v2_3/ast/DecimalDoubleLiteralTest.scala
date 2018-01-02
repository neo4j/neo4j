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

class DecimalDoubleLiteralTest extends CypherFunSuite {
  test("correctly parses decimal numbers") {
    assert(DecimalDoubleLiteral("22.34")(DummyPosition(0)).value === 22.34)
    assert(DecimalDoubleLiteral("-2342.34")(DummyPosition(0)).value === -2342.34)
    assert(DecimalDoubleLiteral("0.34")(DummyPosition(0)).value === 0.34)
    assert(DecimalDoubleLiteral("-.23")(DummyPosition(0)).value === -.23)
    assert(DecimalDoubleLiteral("0.0")(DummyPosition(0)).value === 0.0)
    assert(DecimalDoubleLiteral("-0.0")(DummyPosition(0)).value === 0.0)
    assert(DecimalDoubleLiteral("1E23")(DummyPosition(0)).value === 1E23)
    assert(DecimalDoubleLiteral("1e23")(DummyPosition(0)).value === 1E23)
    assert(DecimalDoubleLiteral("-134E233")(DummyPosition(0)).value === -134E233)
    assert(DecimalDoubleLiteral("-134e233")(DummyPosition(0)).value === -134E233)
    assert(DecimalDoubleLiteral("1E-99")(DummyPosition(0)).value === 1E-99)
    assert(DecimalDoubleLiteral("1e-99")(DummyPosition(0)).value === 1E-99)
    assert(DecimalDoubleLiteral("-4E-593")(DummyPosition(0)).value === -4E-593)
    assert(DecimalDoubleLiteral("-4e-593")(DummyPosition(0)).value === -4E-593)
    assert(DecimalDoubleLiteral("3.42E34")(DummyPosition(0)).value === 3.42E34)
    assert(DecimalDoubleLiteral("3.42e34")(DummyPosition(0)).value === 3.42E34)
    assert(DecimalDoubleLiteral("-65.342546547E33")(DummyPosition(0)).value === -65.342546547E33)
    assert(DecimalDoubleLiteral("-65.342546547e33")(DummyPosition(0)).value === -65.342546547E33)
    assert(DecimalDoubleLiteral("73.234E-235")(DummyPosition(0)).value === 73.234E-235)
    assert(DecimalDoubleLiteral("73.234e-235")(DummyPosition(0)).value === 73.234E-235)
    assert(DecimalDoubleLiteral("-73.234E-235")(DummyPosition(0)).value === -73.234E-235)
    assert(DecimalDoubleLiteral("-73.234e-235")(DummyPosition(0)).value === -73.234E-235)
  }

  test("throws error for invalid decimal numbers") {
    assertSemanticError("33..34", "invalid literal number")
    assertSemanticError("3f.34", "invalid literal number")
    assertSemanticError("3._4", "invalid literal number")
    assertSemanticError("2EE4", "invalid literal number")
    assertSemanticError("2eE4", "invalid literal number")
    assertSemanticError("2Ee4", "invalid literal number")
    assertSemanticError("2ee4", "invalid literal number")
    assertSemanticError("2E--4", "invalid literal number")
    assertSemanticError("2e--4", "invalid literal number")
    assertSemanticError("2E", "invalid literal number")
    assertSemanticError("2e", "invalid literal number")
    assertSemanticError("2..3E34", "invalid literal number")
    assertSemanticError("2..3e34", "invalid literal number")
  }

  test("throws error for too large decimal numbers") {
    assertSemanticError("1E9999", "floating point number is too large")
    assertSemanticError("1e9999", "floating point number is too large")
  }

  private def assertSemanticError(stringValue: String, errorMessage: String) {
    val literal = DecimalDoubleLiteral(stringValue)(DummyPosition(4))
    val result = literal.semanticCheck(SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, DummyPosition(4))))
  }
}
