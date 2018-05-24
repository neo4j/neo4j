/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.lang.Math.PI

import org.neo4j.values.storable.{DoubleValue, Values}
import org.neo4j.values.storable.Values.{NO_VALUE, doubleValue, longValue, stringValue}
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.{EMPTY_MAP, map}
import org.opencypher.v9_0.ast.AstConstructionTestSupport
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{InputPosition, NonEmptyList, symbols}


class CodeGenerationTest extends CypherFunSuite with AstConstructionTestSupport{
  test("round function") {
    // Given
    val expression = function("round", literalFloat(PI))

    // When
    val compiled = compile(expression)

    // Then
    compiled.compute(EMPTY_MAP) should equal(doubleValue(3.0))
  }

  test("sin function") {
    // Given
    val expression = function("sin", literalFloat(PI))

    // When
    val compiled = compile(expression)

    // Then
    compiled.compute(EMPTY_MAP) should equal(doubleValue(Math.sin(PI)))
  }

  test("rand function") {
    // Given
    val expression = function("rand")

    // When
    val compiled = compile(expression)

    // Then
    val value = compiled.compute(EMPTY_MAP).asInstanceOf[DoubleValue].doubleValue()
    value should (be > 0.0 and be <= 1.0)
  }

  test("add function") {
    // Given
    val expression = add(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.compute(EMPTY_MAP) should equal(longValue(52))
  }

  test("subtract function") {
    // Given
    val expression = subtract(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.compute(EMPTY_MAP) should equal(longValue(32))
  }

  test("multiply function") {
    // Given
    val expression = multiply(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.compute(EMPTY_MAP) should equal(longValue(420))
  }

  test("extract parameter") {
    // Given
    val expression = parameter("prop")

    // When
    val compiled = compile(expression)

    // Then
    compiled.compute(EMPTY_MAP) should equal(NO_VALUE)
    compiled.compute(map(Array("prop"), Array(stringValue("foo")))) should equal(stringValue("foo"))
  }

  private def compile(e: Expression) =
    CodeGeneration.compile(IntermediateCodeGeneration.compile(e).getOrElse(fail()))

  private def function(name: String, e: Expression) =
    FunctionInvocation(FunctionName(name)(pos), e)(pos)

  private def function(name: String) =
    FunctionInvocation(Namespace()(pos), FunctionName(name)(pos), distinct = false, IndexedSeq.empty)(pos)

  private def add(l: Expression, r: Expression) = Add(l, r)(pos)

  private def subtract(l: Expression, r: Expression) = Subtract(l, r)(pos)

  private def multiply(l: Expression, r: Expression) = Multiply(l, r)(pos)

  private def parameter(key: String) = Parameter(key, symbols.CTAny)(pos)
}
