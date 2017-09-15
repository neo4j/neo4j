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

import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.symbols._

class LiteralTest extends SemanticFunSuite {
  test("has type CTString") {
    val literal = StringLiteral("foo")(pos)
    val result = SemanticAnalysis.semanticCheck(Expression.SemanticContext.Simple, literal)(SemanticState.clean)
    val expressionType = result.state.expressionType(literal).actual

    assert(expressionType === CTString.invariant)
  }

  test("correctly parses unsigned decimal numbers") {
    assert(UnsignedDecimalIntegerLiteral("22")(pos).value === 22)
    assert(UnsignedDecimalIntegerLiteral("0")(pos).value === 0)
  }

  test("throws error for invalid unsigned decimal numbers") {
    assertSemanticError(unsignedDecimal("12g3"), "invalid literal number")
    assertSemanticError(unsignedDecimal("923_23"), "invalid literal number")
  }

  test("throws error for too large unsigned decimal numbers") {
    assertSemanticError(unsignedDecimal("999999999999999999999999999"), "integer is too large")
  }

  test("correctly parses signed decimal numbers") {
    assert(signedDecimal("22").value === 22)
    assert(signedDecimal("0").value === 0)
    assert(signedDecimal("-0").value === 0)
    assert(signedDecimal("-432").value === -432)
  }

  test("throws error for invalid signed decimal numbers") {
    assertSemanticError(signedDecimal("12g3"), "invalid literal number")
    assertSemanticError(signedDecimal("923_23"), "invalid literal number")
    assertSemanticError(signedDecimal("-92f3"), "invalid literal number")
  }

  test("throws error for too large signed decimal numbers") {
    assertSemanticError(signedDecimal("999999999999999999999999999"), "integer is too large")
  }

  test("correctly parses decimal double numbers") {
    assert(decimalDouble("22.34").value === 22.34)
    assert(decimalDouble("-2342.34").value === -2342.34)
    assert(decimalDouble("0.34").value === 0.34)
    assert(decimalDouble("-.23").value === -.23)
    assert(decimalDouble("0.0").value === 0.0)
    assert(decimalDouble("-0.0").value === 0.0)
    assert(decimalDouble("1E23").value === 1E23)
    assert(decimalDouble("1e23").value === 1E23)
    assert(decimalDouble("-134E233").value === -134E233)
    assert(decimalDouble("-134e233").value === -134E233)
    assert(decimalDouble("1E-99").value === 1E-99)
    assert(decimalDouble("1e-99").value === 1E-99)
    assert(decimalDouble("-4E-593").value === -4E-593)
    assert(decimalDouble("-4e-593").value === -4E-593)
    assert(decimalDouble("3.42E34").value === 3.42E34)
    assert(decimalDouble("3.42e34").value === 3.42E34)
    assert(decimalDouble("-65.342546547E33").value === -65.342546547E33)
    assert(decimalDouble("-65.342546547e33").value === -65.342546547E33)
    assert(decimalDouble("73.234E-235").value === 73.234E-235)
    assert(decimalDouble("73.234e-235").value === 73.234E-235)
    assert(decimalDouble("-73.234E-235").value === -73.234E-235)
    assert(decimalDouble("-73.234e-235").value === -73.234E-235)
  }

  test("throws error for invalid decimal double numbers") {
    assertSemanticError(decimalDouble("33..34"), "invalid literal number")
    assertSemanticError(decimalDouble("3f.34"), "invalid literal number")
    assertSemanticError(decimalDouble("3._4"), "invalid literal number")
    assertSemanticError(decimalDouble("2EE4"), "invalid literal number")
    assertSemanticError(decimalDouble("2eE4"), "invalid literal number")
    assertSemanticError(decimalDouble("2Ee4"), "invalid literal number")
    assertSemanticError(decimalDouble("2ee4"), "invalid literal number")
    assertSemanticError(decimalDouble("2E--4"), "invalid literal number")
    assertSemanticError(decimalDouble("2e--4"), "invalid literal number")
    assertSemanticError(decimalDouble("2E"), "invalid literal number")
    assertSemanticError(decimalDouble("2e"), "invalid literal number")
    assertSemanticError(decimalDouble("2..3E34"), "invalid literal number")
    assertSemanticError(decimalDouble("2..3e34"), "invalid literal number")
  }

  test("throws error for too large decimal numbers") {
    assertSemanticError(decimalDouble("1E9999"), "floating point number is too large")
    assertSemanticError(decimalDouble("1e9999"), "floating point number is too large")
  }

  private def assertSemanticError(literal: Literal, errorMessage: String) {
    val result = SemanticAnalysis.semanticCheck(SemanticContext.Simple, literal)(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, pos)))
  }
}
