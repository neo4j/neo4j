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
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue
import org.scalacheck.Gen

class ToStringFunctionTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {

  val tests: Seq[(Any => AnyValue, String)] =
    Seq((toString, "toString"), (toStringOrNull, "toStringOrNull"))

  tests.foreach { case (toStringFn, name) =>
    test(s"$name: should return null if argument is null") {
      assert(toStringFn(null) === NO_VALUE)
    }

    test(s"$name: should not change a string") {
      toStringFn("10.599") should be(stringValue("10.599"))
    }

    test(s"$name:  can handle empty string as null") {
      toStringFn("") should be(stringValue(""))
    }

    test(s"$name: should convert an integer to a string") {
      toStringFn(21) should be(stringValue("21"))
    }

    test(s"$name: should convert a float to a string") {
      toStringFn(23.34) should be(stringValue("23.34"))
    }

    test(s"$name: should convert a negative float to a string") {
      toStringFn(-12.66) should be(stringValue("-12.66"))
    }

    test(s"$name: should convert a negative integer to a string") {
      toStringFn(-12) should be(stringValue("-12"))
    }

    test(s"$name: should handle boolean false") {
      toStringFn(false) should be(stringValue("false"))
    }

    test(s"$name: should handle boolean true") {
      toStringFn(true) should be(stringValue("true"))
    }
  }

  // toString

  test("should throw an exception if the argument is an object which cannot be converted to a string") {
    val caughtException = the[CypherTypeException] thrownBy toString(List(1, 24))
    caughtException.getMessage should startWith(
      "Invalid input for function 'toString()': Expected a String, Float, Integer, Boolean, Temporal or Duration, got: "
    )
  }

  // toStringOrNull

  test("toStringOrNull can handle map type as null") {
    toStringOrNull(Map("a" -> "b")) should equal(NO_VALUE)
  }

  test("toStringOrNull can handle list types as null") {
    toStringOrNull(List("a", "b")) should equal(NO_VALUE)
  }

  test("toStringOrNull can handle empty map as null") {
    toStringOrNull(Map.empty) should equal(NO_VALUE)
  }

  test("toStringOrNull can handle empty list as null") {
    toStringOrNull(List.empty) should equal(NO_VALUE)
  }

  test("toStringOrNull should not throw an exception for any value") {
    val generator: Gen[Any] = Gen.oneOf[Any](Gen.numStr, Gen.alphaStr, Gen.posNum[Double], Gen.posNum[Int])

    forAll(generator) { s =>
      {
        toStringOrNull(s) should (be(a[TextValue]) or equal(NO_VALUE))
      }
    }
  }

  private def toString(orig: Any) = {
    ToStringFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }

  private def toStringOrNull(orig: Any) = {
    ToStringOrNullFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }
}
