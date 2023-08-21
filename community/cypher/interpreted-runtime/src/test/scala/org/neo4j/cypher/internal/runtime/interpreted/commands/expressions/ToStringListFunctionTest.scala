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
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues
import org.scalacheck.Gen
import org.scalatest.Inspectors

class ToStringListFunctionTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {

  test("should return null if argument is null") {
    assert(toStringList(null) === NO_VALUE)
  }

  test("should return [] if argument is []") {
    assert(toStringList(Seq[String]()) === VirtualValues.EMPTY_LIST)
  }

  test("should convert a list of integers to a list of strings") {
    assert(toStringList(Seq[Int](123, 456)) === Values.stringArray("123", "456"))
  }

  test("should convert a list of strings to a list of strings") {
    assert(toStringList(Seq[String]("123", "456")) === Values.stringArray("123", "456"))
  }

  test("should convert an array of strings to a list of string") {
    assert(toStringList(Array("123", "456")) === Values.stringArray("123", "456"))
  }

  test("should convert a list of doubles to a list of strings") {
    assert(toStringList(Seq(123.0d, 456.5d)) === Values.stringArray("123.0", "456.5"))
  }

  test("should convert a list of floats to a list of strings") {
    assert(toStringList(Seq(123.0f, 456.9f)) === Values.stringArray("123.0", "456.9"))
  }

  test("should convert a mixed list to a list of strings") {
    val expected = Values.stringArray("123.0", "456.9", "foo", "789")
    assert(toStringList(Seq(123.0f, 456.9d, "foo", "789")) === expected)
  }

  test("should convert a list of booleans to a list of strings") {
    assert(toStringList(Seq(true, false)) === Values.stringArray("true", "false"))
  }

  test("should not throw an exception if the list argument contains an object which cannot be converted to string") {
    assert(toStringList(Seq("1234", VirtualValues.EMPTY_MAP, VirtualValues.EMPTY_LIST)) === VirtualValues.list(
      stringValue("1234"),
      NO_VALUE,
      NO_VALUE
    ))
  }

  test("should throw an exception if the list argument contains a non-list") {
    val caughtException = the[CypherTypeException] thrownBy toStringList("foo")
    caughtException.getMessage should equal(
      """Invalid input for function 'toStringList()': Expected a List, got: String("foo")"""
    )
  }

  test("should not throw an exception for any value in the list") {
    val generator: Gen[List[Any]] = Gen.listOf(Gen.oneOf(Gen.numStr, Gen.alphaStr, Gen.posNum[Double]))
    forAll(generator) { s =>
      {
        import scala.jdk.CollectionConverters.IterableHasAsScala
        val result = toStringList(s)
        Inspectors.forAll(result.asInstanceOf[ListValue].asScala) { _ should (be(a[TextValue]) or equal(NO_VALUE)) }
      }
    }
  }

  private def toStringList(orig: Any) = {
    ToStringListFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }
}
