/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.functionArgumentGqlException
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.storable.Values.float32Vector
import org.neo4j.values.storable.Values.float64Vector
import org.neo4j.values.storable.Values.int16Vector
import org.neo4j.values.storable.Values.int32Vector
import org.neo4j.values.storable.Values.int64Vector
import org.neo4j.values.storable.Values.int8Vector
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues
import org.scalacheck.Gen
import org.scalatest.Inspectors

class ToFloatListFunctionTest extends InterpretedRuntimeTestSuite with CypherScalaCheckDrivenPropertyChecks {

  test("should return null if argument is null") {
    assert(toFloatList(null) === NO_VALUE)
  }

  test("should return [] if argument is []") {
    assert(toFloatList(Seq[String]()) === Values.doubleArray(Array()))
  }

  test("should convert a list of integers to a list of floats") {
    assert(toFloatList(Seq[Int](123, 456)) === Values.doubleArray(Array(123d, 456d)))
  }

  test("should convert an array of integers to a list of floats") {
    assert(toFloatList(Array(123, 456)) === Values.doubleArray(Array(123d, 456d)))
  }

  test("should convert a list of strings to a list of floats") {
    assert(toFloatList(Seq[String]("123.2", "456")) === Values.doubleArray(Array(123.2d, 456d)))
  }

  test("should convert a list of doubles to a list of floats") {
    assert(toFloatList(Seq(123.0d, 456.5d)) === Values.doubleArray(Array(123d, 456.5d)))
  }

  test("should convert a list of floats to a list of floats") {
    assert(toFloatList(Seq(123.0f, 456.9f)) === Values.doubleArray(Array(123.0f, 456.9f)))
  }

  test("should convert a mixed list to a list of floats") {
    assert(toFloatList(Seq[Any](123.0f, 456.9d, "789")) === Values.doubleArray(Array(123d, 456.9d, 789d)))
  }

  test("should convert a mixed list with invalid strings to a list of floats") {
    val expected = VirtualValues.list(doubleValue(123), doubleValue(456.9), NO_VALUE, doubleValue(789))
    assert(toFloatList(Seq[Any](123.0f, 456.9d, "foo", "789")) === expected)
  }

  test("should convert a mixed list of strings to a list of floats") {
    assert(toFloatList(Seq("0000123", "-456", "-1.789")) === Values.doubleArray(Array(123d, -456d, -1.789d)))
  }

  test("should convert a vector value to a list of integers") {
    assert(toFloatList(int8Vector(1, 2, 3, 4, 5)) === Values.doubleArray(Array(1, 2, 3, 4, 5)))
    assert(toFloatList(int16Vector(1, 2, 3, 4, 5)) === Values.doubleArray(Array(1, 2, 3, 4, 5)))
    assert(toFloatList(int32Vector(1, 2, 3, 4, 5)) === Values.doubleArray(Array(1, 2, 3, 4, 5)))
    assert(toFloatList(int64Vector(1, 2, 3, 4, 5)) === Values.doubleArray(Array(1, 2, 3, 4, 5)))
    assert(toFloatList(float32Vector(1, 2, 3, 4, 5)) === Values.doubleArray(Array(1, 2, 3, 4, 5)))
    assert(toFloatList(float64Vector(1, 2, 3, 4, 5)) === Values.doubleArray(Array(1, 2, 3, 4, 5)))
  }

  test("should not convert a list of booleans to a list of floats") {
    assert(toFloatList(Seq(true, false)) === VirtualValues.list(NO_VALUE, NO_VALUE))
  }

  test("should convert a list with a large number to a list of floats") {
    val tooBigFloat = BigDecimal(Double.MaxValue) * 1.5 // this will evaluate to infinity
    assert(toFloatList(Seq("1", tooBigFloat.toString(), "8589934592.0")) == VirtualValues.list(
      doubleValue(1),
      doubleValue(tooBigFloat.toDouble),
      doubleValue(8589934592L)
    ))
  }

  test("should not throw an exception if the list argument contains an object which cannot be converted to float") {
    assert(toFloatList(
      Seq("1234", Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 0))
    ) === VirtualValues.list(doubleValue(1234), NO_VALUE))
  }

  test("should throw an exception if the list argument contains a non-list") {
    val caughtException = the[CypherTypeException] thrownBy toFloatList("foo")
    caughtException should be(functionArgumentGqlException(
      "Invalid input for function 'toFloatList()': Expected a List or Vector, got: String(\"foo\")",
      "toFloatList()",
      "Expected the value \"foo\" to be of type LIST<ANY> or VECTOR, but was of type STRING NOT NULL."
    ))
  }

  test("should not throw an exception for any value in the list") {
    val generator: Gen[List[Any]] = Gen.listOf(Gen.oneOf(Gen.numStr, Gen.alphaStr, Gen.posNum[Double]))
    forAll(generator) { s =>
      {
        import scala.jdk.CollectionConverters.IterableHasAsScala
        val result = toFloatList(s)
        Inspectors.forAll(result.asInstanceOf[ListValue].asScala) { _ should (be(a[DoubleValue]) or equal(NO_VALUE)) }
      }
    }
  }

  private def toFloatList(orig: Any) = {
    ToFloatListFunctionCypher25(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }
}
