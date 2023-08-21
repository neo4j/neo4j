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
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues
import org.scalacheck.Gen
import org.scalatest.Inspectors
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ToBooleanListFunctionTest extends CypherFunSuite with ScalaCheckDrivenPropertyChecks {

  test("should return null if argument is null") {
    assert(toBooleanList(null) === NO_VALUE)
  }

  test("should return [] if argument is []") {
    assert(toBooleanList(Seq[String]()) === Values.booleanArray(Array()))
  }

  test("should convert a list of integers to a list of booleans") {
    assert(toBooleanList(Seq[Int](123, 0)) === Values.booleanArray(Array(true, false)))
  }

  test("should convert a list of strings to a list of booleans") {
    assert(toBooleanList(Seq[String]("true", "false")) === Values.booleanArray(Array(true, false)))
  }

  test("should convert an array of strings to a list of booleans") {
    assert(toBooleanList(Array("true", "false")) === Values.booleanArray(Array(true, false)))
  }

  test("should not convert a list of doubles to a list of booleans") {
    assert(toBooleanList(Seq(123.0d, 456.5d)) === VirtualValues.list(NO_VALUE, NO_VALUE))
  }

  test("should not convert a list of floats to a list of booleans") {
    assert(toBooleanList(Seq(123.0f, 456.9f)) === VirtualValues.list(NO_VALUE, NO_VALUE))
  }

  test("should convert a mixed list to a list of booleans") {
    assert(toBooleanList(Seq(0, true, "false")) === VirtualValues.list(Values.FALSE, Values.TRUE, Values.FALSE))
  }

  test("should convert a mixed list that also include invalid strings to a list of booleans") {
    val expected = VirtualValues.list(Values.TRUE, Values.TRUE, NO_VALUE, Values.FALSE)
    assert(toBooleanList(Seq(123, "trUE", "foo", false)) === expected)
  }

  test("should convert a mixed list of strings to a list of booleans") {
    assert(toBooleanList(Seq("0000123", "-456", "false")) === VirtualValues.list(NO_VALUE, NO_VALUE, Values.FALSE))
  }

  test("should convert a list of booleans to a list of booleans") {
    assert(toBooleanList(Seq(true, false)) === VirtualValues.list(Values.TRUE, Values.FALSE))
  }

  test("should not throw an exception if the list argument contains an object which cannot be converted to booleans") {
    assert(toBooleanList(
      Seq("true", Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 0))
    ) === VirtualValues.list(Values.TRUE, NO_VALUE))
  }

  test("should throw an exception if the list argument contains a non-list") {
    val caughtException = the[CypherTypeException] thrownBy toBooleanList("foo")
    caughtException.getMessage should equal(
      """Invalid input for function 'toBooleanList()': Expected a List, got: String("foo")"""
    )
  }

  test("should not throw an exception for any value in the list") {
    val generator: Gen[List[Any]] = Gen.listOf(Gen.oneOf(Gen.numStr, Gen.alphaStr, Gen.posNum[Double]))
    forAll(generator) { s =>
      {
        import scala.jdk.CollectionConverters.IterableHasAsScala
        val result = toBooleanList(s)
        Inspectors.forAll(result.asInstanceOf[ListValue].asScala) { _ should (be(a[BooleanValue]) or equal(NO_VALUE)) }
      }
    }
  }

  private def toBooleanList(orig: Any): Any = {
    ToBooleanListFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }
}
