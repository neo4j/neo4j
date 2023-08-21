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
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values.FALSE
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.TRUE
import org.scalacheck.Gen

class ToBooleanFunctionTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {

  val tests: Seq[(Any => AnyValue, String)] =
    Seq((toBoolean, "toBoolean"), (toBooleanOrNull, "toBooleanOrNull"))

  tests.foreach { case (toBooleanFn, name) =>
    test(s"$name: null in null out") {
      assert(toBooleanFn(null) === NO_VALUE)
    }

    test(s"$name: converts strings to booleans") {
      Seq("true  ", "TRUE", " tRuE").foreach { s =>
        toBooleanFn(s) shouldBe TRUE
      }
      Seq("false", " FALSE", "FaLsE  ").foreach { s =>
        toBooleanFn(s) shouldBe FALSE
      }
    }

    test(s"$name: identity for booleans") {
      toBooleanFn(true) shouldBe TRUE
      toBooleanFn(false) shouldBe FALSE
    }

    test(s"$name: null for bad strings") {
      toBooleanFn("tru") shouldBe NO_VALUE
      toBooleanFn("") shouldBe NO_VALUE
    }

    test(s"$name: converts integers to booleans") {
      toBooleanFn(0) shouldBe FALSE
      toBooleanFn(1) shouldBe TRUE
      toBooleanFn(Integer.MAX_VALUE) shouldBe TRUE
      toBooleanFn(Integer.MIN_VALUE) shouldBe TRUE
    }

    test(s"$name: converts longs to booleans") {
      toBooleanFn(0L) shouldBe FALSE
      toBooleanFn(1L) shouldBe TRUE
      toBooleanFn(Long.MaxValue) shouldBe TRUE
      toBooleanFn(Long.MinValue) shouldBe TRUE
    }
  }

  // toBoolean

  test("toBoolean throws for wrong types") {
    a[CypherTypeException] shouldBe thrownBy(toBoolean(1.1))
    val toBooleanOfList = ToBooleanFunction(ListLiteral.empty)
    a[CypherTypeException] shouldBe thrownBy(toBooleanOfList(CypherRow.empty, QueryStateHelper.empty))
  }

  // toBooleanOrNull

  test("toBooleanOrNull can handle map type as null") {
    toBooleanOrNull(Map("a" -> "b")) should equal(NO_VALUE)
  }

  test("toBooleanOrNull can handle empty string as null") {
    toBooleanOrNull("") should equal(NO_VALUE)
  }

  test("toBooleanOrNull can handle float as null") {
    toBooleanOrNull(4.3f) should equal(NO_VALUE)
  }

  test("toBooleanOrNull can handle double as null") {
    toBooleanOrNull(4.3d) should equal(NO_VALUE)
  }

  test("toBooleanOrNull can handle unrecognised string as null") {
    toBooleanOrNull("foo bar") should equal(NO_VALUE)
  }

  test("toBooleanOrNull can handle list types as null") {
    toBooleanOrNull(List("a", "b")) should equal(NO_VALUE)
  }

  test("toBooleanOrNull can handle empty map as null") {
    toBooleanOrNull(Map.empty) should equal(NO_VALUE)
  }

  test("toBooleanOrNull can handle empty list as null") {
    toBooleanOrNull(List.empty) should equal(NO_VALUE)
  }

  test("toBooleanOrNull should not throw an exception for any value") {
    val generator: Gen[Any] = Gen.oneOf[Any](Gen.numStr, Gen.alphaStr, Gen.posNum[Double])

    forAll(generator) { s =>
      {
        toBooleanOrNull(s) should (be(a[BooleanValue]) or equal(NO_VALUE))
      }
    }
  }

  private def toBoolean(orig: Any) = {
    ToBooleanFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }

  private def toBooleanOrNull(orig: Any) = {
    ToBooleanOrNullFunction(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }

}
