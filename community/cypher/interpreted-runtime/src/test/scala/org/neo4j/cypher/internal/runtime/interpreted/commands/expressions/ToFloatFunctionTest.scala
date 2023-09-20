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
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.doubleValue
import org.scalacheck.Gen

class ToFloatFunctionTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {

  val tests: Seq[(Any => AnyValue, String)] =
    Seq((toFloat, "toFloat"), (toFloatOrNull, "toFloatOrNull"))

  tests.foreach { case (toFloatFn, name) =>
    test(s"$name should return null if argument is null") {
      assert(toFloatFn(null) === NO_VALUE)
    }

    test(s"$name should convert a string to a float") {
      toFloatFn("10.599") should be(doubleValue(10.599))
    }

    test(s"$name should convert an integer string to a float") {
      toFloatFn("21") should be(doubleValue(21.0))
    }

    test(s"$name should convert an integer to a float") {
      toFloatFn(23) should be(doubleValue(23.0))
    }

    test(s"$name should return null if the argument is a partially numeric string") {
      assert(toFloatFn("20foobar2") === NO_VALUE)
    }

    test(s"$name should convert a string with leading zeros to a float") {
      toFloatFn("000123121.5") should be(doubleValue(123121.5))
    }

    test(s"$name should convert a string with leading minus to a negative float") {
      toFloatFn("-12.66") should be(doubleValue(-12.66))
    }

    test(s"$name should convert a string with leading minus and zeros to a negative float") {
      toFloatFn("-00012.91") should be(doubleValue(-12.91))
    }

    test(s"given a float $name should give the same value back") {
      toFloatFn(50.5) should be(doubleValue(50.5))
    }
  }

  // toFloat

  test("should throw an exception if the argument is an object which cannot be converted to a float") {
    val caughtException = the[CypherTypeException] thrownBy toFloat(true)
    caughtException.getMessage should startWith(
      "Invalid input for function 'toFloat()': Expected a String, Float or Integer, got: "
    )
  }

  // toFloatOrNull

  test("toFloatOrNull can handle map type as null") {
    toFloatOrNull(Map("a" -> "b")) should equal(NO_VALUE)
  }

  test("toFloatOrNull can handle empty string as null") {
    toFloatOrNull("") should equal(NO_VALUE)
  }

  test("toFloatOrNull can handle boolean as null") {
    toFloatOrNull(true) should equal(NO_VALUE)
  }

  test("toFloatOrNull can handle unrecognised string as null") {
    toFloatOrNull("foo bar") should equal(NO_VALUE)
  }

  test("toFloatOrNull can handle list types as null") {
    toFloatOrNull(List("a", "b")) should equal(NO_VALUE)
  }

  test("toFloatOrNull can handle empty map as null") {
    toFloatOrNull(Map.empty) should equal(NO_VALUE)
  }

  test("toFloatOrNull can handle empty list as null") {
    toFloatOrNull(List.empty) should equal(NO_VALUE)
  }

  test(s"toFloatOrNull should handle integers larger that 8 bytes") {
    toFloatOrNull("10508455564958384115") should equal(doubleValue(10508455564958384115d))
  }

  test(s"toFloatOrNull handles infinity") {
    toFloatOrNull((BigDecimal(Double.MaxValue) * 2).toString) should equal(doubleValue(Double.PositiveInfinity))
  }

  test(s"toFloatOrNull handles negative infinity") {
    toFloatOrNull((BigDecimal(Double.MaxValue) * (-2)).toString) should equal(doubleValue(Double.NegativeInfinity))
  }

  test("toFloatOrNull should not throw an exception for any value") {
    val generator: Gen[Any] = Gen.oneOf[Any](Gen.numStr, Gen.alphaStr, Gen.posNum[Double])

    forAll(generator) { s =>
      {
        toFloatOrNull(s) should (be(a[DoubleValue]) or equal(NO_VALUE))
      }
    }
  }

  private def toFloat(orig: Any) = {
    ToFloatFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }

  private def toFloatOrNull(orig: Any) = {
    ToFloatOrNullFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }
}
