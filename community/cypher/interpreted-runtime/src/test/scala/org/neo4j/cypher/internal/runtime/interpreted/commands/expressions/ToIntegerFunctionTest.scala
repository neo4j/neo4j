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
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlException
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.longValue
import org.scalacheck.Gen

class ToIntegerFunctionTest extends InterpretedRuntimeTestSuite with CypherScalaCheckDrivenPropertyChecks {

  val tests: Seq[(Any => AnyValue, String)] =
    Seq((toInteger, "toInteger"), (toIntegerOrNull, "toIntegerOrNull"))

  tests.foreach { case (toIntegerFn, name) =>
    test(s"$name should return null if argument is null") {
      assert(toIntegerFn(null) === NO_VALUE)
    }

    test(s"$name should convert a string to an integer") {
      toIntegerFn("10") should equal(longValue(10))
    }

    test(s"$name should convert a double to an integer") {
      toIntegerFn(23.5d) should equal(longValue(23))
    }

    test(s"$name should parse float and truncate to int if the argument is a float literal") {
      toIntegerFn("20.5") should equal(longValue(20))
    }

    test(s"$name should return null if the argument is a partially numeric string") {
      assert(toIntegerFn("20foobar2") === NO_VALUE)
    }

    test(s"$name should not return null if the argument is a hexadecimal string") {
      assert(toIntegerFn("0x20") === longValue(0x20))
    }

    test(s"$name should not return null if the argument is a octal string") {
      assert(toIntegerFn("0o11") === longValue(9))
    }

    test(s"$name should not return null if the argument has underscore separator") {
      assert(toIntegerFn("1_123_456") === longValue(1_123_456))
    }

    test(s"$name should convert a string with leading zeros to an integer") {
      toIntegerFn("000123121") should equal(longValue(123121))
    }

    test(s"$name should convert a string with leading minus in a negative integer") {
      toIntegerFn("-12") should equal(longValue(-12))
    }

    test(s"$name should convert a string with leading minus and zeros in a negative integer") {
      toIntegerFn("-00012") should equal(longValue(-12))
    }

    test(s"$name given an integer should give the same value back") {
      toIntegerFn(50) should equal(longValue(50))
    }

    test(s"$name should truncate floats if given a float") {
      toIntegerFn(20.6f) should equal(longValue(20))
    }

    test(s"$name should handle floats larger than 2^31 - 1") {
      // 2^33 = 8589934592
      toIntegerFn("8589934592.0") should equal(longValue(8589934592L))
    }

    test(s"$name should handle -2^63") {
      toIntegerFn("-9223372036854775808") should equal(longValue(Long.MinValue))
    }

    test(s"$name should handle 2^63 - 1") {
      toIntegerFn("9223372036854775807") should equal(longValue(Long.MaxValue))
    }

    test(s"$name should handle true") {
      toIntegerFn(true) should equal(longValue(1))
    }

    test(s"$name should can handle false") {
      toIntegerFn(false) should equal(longValue(0))
    }

    test(s"$name returns null on addition") {
      toIntegerFn("1+1") should equal(NO_VALUE)
    }
  }

  test(s"toInteger should fail for larger integers larger that 8 bytes") {
    the[CypherTypeException] thrownBy toInteger("10508455564958384115") should be(gqlException(
      "integer, 10508455564958384115, is too large",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_22003,
        "error: data exception - numeric value out of range. The numeric value 10508455564958384115 is outside the required range."
      ).withCause(
        GqlStatusInfoCodes.STATUS_22N03,
        "error: data exception - specified numeric value out of range. Expected 'input' to be of type INTEGER and in the range -9223372036854775808 to 9223372036854775807 but found 10508455564958384115."
      )
    ))
  }

  test(s"toInteger cannot handle -2^63-1") {
    the[CypherTypeException] thrownBy toInteger("-9223372036854775809") should be(gqlException(
      "integer, -9223372036854775809, is too large",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_22003,
        "error: data exception - numeric value out of range. The numeric value -9223372036854775809 is outside the required range."
      ).withCause(
        GqlStatusInfoCodes.STATUS_22N03,
        "error: data exception - specified numeric value out of range. Expected 'input' to be of type INTEGER and in the range -9223372036854775808 to 9223372036854775807 but found -9223372036854775809."
      )
    ))
  }

  test(s"toInteger cannot handle 2^63") {
    the[CypherTypeException] thrownBy toInteger("9223372036854775808") should be(gqlException(
      "integer, 9223372036854775808, is too large",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_22003,
        "error: data exception - numeric value out of range. The numeric value 9223372036854775808 is outside the required range."
      ).withCause(
        GqlStatusInfoCodes.STATUS_22N03,
        "error: data exception - specified numeric value out of range. Expected 'input' to be of type INTEGER and in the range -9223372036854775808 to 9223372036854775807 but found 9223372036854775808."
      )
    ))
  }

  test(
    "toInteger should throw an exception if the argument is an object which cannot be converted to integer"
  ) {
    val caughtException = the[CypherTypeException] thrownBy toInteger(
      Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 0)
    )
    caughtException should be(functionArgumentGqlException(
      "Invalid input for function 'toInteger()': Expected a String, Float, Integer or Boolean, got: point({x: 1.0, y: 0.0, crs: 'cartesian'})",
      "toInteger()",
      "Expected the value point({x: 1.0, y: 0.0, crs: 'cartesian'}) to be of type STRING, FLOAT, INTEGER or BOOLEAN, but was of type POINT NOT NULL."
    ))
  }

  // ToIntegerOrNull

  test("toIntegerOrNull can handle map type as null") {
    toIntegerOrNull(Map("a" -> "b")) should equal(NO_VALUE)
  }

  test("toIntegerOrNull can handle empty string as null") {
    toIntegerOrNull("") should equal(NO_VALUE)
  }

  test("toIntegerOrNull can handle boolean") {
    toIntegerOrNull(true) should equal(longValue(1))
  }

  test("toIntegerOrNull can handle unrecognised string as null") {
    toIntegerOrNull("foo bar") should equal(NO_VALUE)
  }

  test("toIntegerOrNull can handle list types as null") {
    toIntegerOrNull(List("a", "b")) should equal(NO_VALUE)
  }

  test("toIntegerOrNull can handle empty map as null") {
    toIntegerOrNull(Map.empty) should equal(NO_VALUE)
  }

  test("toIntegerOrNull can handle empty list as null") {
    toIntegerOrNull(List.empty) should equal(NO_VALUE)
  }

  test(s"toIntegerOrNull should handle integers larger that 8 bytes as null") {
    toIntegerOrNull("10508455564958384115") should equal(NO_VALUE)
  }

  test(s"toIntegerOrNull handles -2^63-1 as null") {
    toIntegerOrNull("-9223372036854775809") should equal(NO_VALUE)
  }

  test(s"toIntegerOrNull handles 2^63 as null") {
    toIntegerOrNull("9223372036854775808") should equal(NO_VALUE)
  }

  test("toIntegerOrNull should not throw an exception for any value") {
    val generator: Gen[Any] = Gen.oneOf[Any](Gen.numStr, Gen.alphaStr, Gen.posNum[Double])

    forAll(generator) { s =>
      {
        toIntegerOrNull(s) should (be(a[LongValue]) or equal(NO_VALUE))
      }
    }
  }

  private def toInteger(orig: Any) = {
    ToIntegerFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }

  private def toIntegerOrNull(orig: Any) = {
    ToIntegerOrNullFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }
}
