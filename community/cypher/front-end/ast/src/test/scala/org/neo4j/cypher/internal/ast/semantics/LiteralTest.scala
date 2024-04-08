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

import org.neo4j.cypher.internal.ast.SemanticCheckInTest.SemanticCheckWithDefaultContext
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.symbols.CTString

class LiteralTest extends SemanticFunSuite {

  test("has type CTString") {
    val literal = StringLiteral("foo")(pos.withInputLength(0))
    val result = SemanticExpressionCheck.simple(literal).run(SemanticState.clean)
    val expressionType = result.state.expressionType(literal).actual

    assert(expressionType === CTString.invariant)
  }

  test("correctly parses unsigned decimal numbers") {
    assert(UnsignedDecimalIntegerLiteral("22")(pos).value === 22)
    assert(UnsignedDecimalIntegerLiteral("0")(pos).value === 0)
  }

  test("throws error for invalid unsigned decimal numbers") {
    assertSemanticError(unsignedDecimal("12g3"), "invalid literal number")
    assertSemanticError(unsignedDecimal("92323_"), "invalid literal number")
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
    assertSemanticError(signedDecimal("92323_"), "invalid literal number")
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
    // In scala 2.11 we could handle E-593, but in scala 2.12 we can only handle E-323
    assert(decimalDouble("-4E-323").value === -4E-323)
    assert(decimalDouble("-4e-323").value === -4E-323)
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

  test("correctly parses old syntax octal numbers") {
    assert(signedOctal("022").value === 0x12)
    assert(signedOctal("00").value === 0x0)
    assert(signedOctal("0734").value === 0x1dc)
    assert(signedOctal("0034").value === 0x1c)
  }

  test("correctly parses hex numbers") {
    assert(signedHex("0x22").value === 0x22)
    assert(signedHex("0x0").value === 0x0)
    assert(signedHex("0x734").value === 0x734)
    assert(signedHex("-0x034").value === -0x034)
  }

  test("correctly parses hex numbers with old syntax") {
    assert(signedHex("0X22").value === 0x22)
    assert(signedHex("0X0").value === 0x0)
    assert(signedHex("0X734").value === 0x734)
    assert(signedHex("-0X034").value === -0x034)
  }

  test("correctly parses octal numbers") {
    assert(signedOctal("0o22").value === 0x12)
    assert(signedOctal("0o0").value === 0x0)
    assert(signedOctal("0o734").value === 0x1dc)
    assert(signedOctal("-0o034").value === -0x1c)
  }

  test("throws error for invalid old syntax octal numbers") {
    assertSemanticError(signedOctal("0393"), "invalid literal number")
    assertSemanticError(signedOctal("03f4"), "invalid literal number")
    assertSemanticError(signedOctal("-0934"), "invalid literal number")
  }

  test("throws error for invalid octal numbers") {
    assertSemanticError(signedOctal("0o393"), "invalid literal number")
    assertSemanticError(signedOctal("0o3f4"), "invalid literal number")
    assertSemanticError(signedOctal("-0o934"), "invalid literal number")
    assertSemanticError(signedOctal("0O393"), "invalid literal number")
    assertSemanticError(signedOctal("0O3f4"), "invalid literal number")
    assertSemanticError(signedOctal("-0O934"), "invalid literal number")
  }

  test("throws error for too large old syntax octal numbers") {
    assertSemanticError(signedOctal("077777777777777777777777777777"), "integer is too large")
  }

  test("throws error for too large octal numbers") {
    assertSemanticError(signedOctal("0o77777777777777777777777777777"), "integer is too large")
  }

  private def assertSemanticError(literal: Literal, errorMessage: String): Unit = {
    val result = SemanticExpressionCheck.simple(literal).run(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, pos)))
  }
}
