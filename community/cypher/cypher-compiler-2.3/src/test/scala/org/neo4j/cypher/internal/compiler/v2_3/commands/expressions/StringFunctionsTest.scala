/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class StringFunctionsTest extends CypherFunSuite {

  // TODO Move these tests into individual classes, at least for the more complex ones

  private val expectedNull = null.asInstanceOf[Any]

  test("replaceTests") {
    def replace(orig: Any, from: Any, to: Any) =
      ReplaceFunction(Literal(orig), Literal(from), Literal(to)).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    replace("hello", "l", "w") should equal("hewwo")
    replace("hello", "ell", "ipp") should equal("hippo")
    replace("hello", "a", "x") should equal("hello")
    replace(null, "a", "x") should equal(expectedNull)
    replace("hello", null, "x") should equal(expectedNull)
    replace("hello", "o", null) should equal(expectedNull)
    intercept[CypherTypeException](replace(1042, "10", "30"))
  }

  test("leftTests") {
    def left(from: Any, r: Any) =
      LeftFunction(Literal(from), Literal(r)).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    left("hello", 2) should equal("he")
    left("hello", 4) should equal("hell")
    left("hello", 8) should equal("hello")
    left(null, 8) should equal(expectedNull)
    intercept[CypherTypeException](left(1042, 2))
    intercept[StringIndexOutOfBoundsException](left("hello", -4))
  }

  test("rightTests") {
    def right(from: Any, r: Any) =
      RightFunction(Literal(from), Literal(r)).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    right("hello", 2) should equal("lo")
    right("hello", 4) should equal("ello")
    right("hello", 8) should equal("hello")
    right(null, 8) should equal(expectedNull)
    intercept[CypherTypeException](right(1024, 2))
    intercept[StringIndexOutOfBoundsException](right("hello", -4))
  }

  test("substringTests") {
    def substring(orig: Any, from: Any, to: Any) =
      SubstringFunction(Literal(orig), Literal(from), Some(Literal(to))).apply(ExecutionContext.empty)(QueryStateHelper.empty)
    def substringFrom(orig: Any, from: Any) =
      SubstringFunction(Literal(orig), Literal(from), None).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    substring("hello", 2, 5) should equal("llo")
    substring("hello", 4, 5) should equal("o")
    substring("hello", 1, 3) should equal("ell")
    substring("hello", 8, 5) should equal("")
    substringFrom("0123456789", 1) should equal("123456789")
    substringFrom("0123456789", 5) should equal("56789")
    substringFrom("0123456789", 15) should equal("")
    substring(null, 8, 5) should equal(expectedNull)
    intercept[CypherTypeException](substring(1024, 1, 2) should equal(expectedNull))
    intercept[StringIndexOutOfBoundsException](substring("hello", -4, 2) should equal(expectedNull))
  }

  test("lowerTests") {
    def lower(x: Any) = LowerFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    lower("HELLO") should equal("hello")
    lower("Hello") should equal("hello")
    lower("hello") should equal("hello")
    lower(null) should equal(expectedNull)
    intercept[CypherTypeException](lower(1024) should equal(expectedNull))
  }

  test("upperTests") {
    def upper(x: Any) = UpperFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    upper("HELLO") should equal("HELLO")
    upper("Hello") should equal("HELLO")
    upper("hello") should equal("HELLO")
    upper(null) should equal(expectedNull)
    intercept[CypherTypeException](upper(1024) should equal(expectedNull))
  }

  test("ltrimTests") {
    def ltrim(x: Any) = LTrimFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    ltrim("  HELLO") should equal("HELLO")
    ltrim(" Hello") should equal("Hello")
    ltrim("  hello") should equal("hello")
    ltrim("  hello  ") should equal("hello  ")
    ltrim(null) should equal(expectedNull)
    intercept[CypherTypeException](ltrim(1024))
  }

  test("rtrimTests") {
    def rtrim(x: Any) = RTrimFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    rtrim("HELLO  ") should equal("HELLO")
    rtrim("Hello   ") should equal("Hello")
    rtrim("  hello   ") should equal("  hello")
    rtrim(null) should equal(expectedNull)
    intercept[CypherTypeException](rtrim(1024))
  }

  test("trimTests") {
    def trim(x: Any) = TrimFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    trim("  hello  ") should equal("hello")
    trim("  hello ") should equal("hello")
    trim("hello  ") should equal("hello")
    trim("  hello  ") should equal("hello")
    trim("  hello") should equal("hello")
    trim(null) should equal(expectedNull)
    intercept[CypherTypeException](trim(1042))
  }

  test("stringTests") {
    def str(x: Any) = StrFunction(Literal(x)).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    str(1234) should equal("1234")
    str(List(1, 2, 3, 4)) should equal("[1,2,3,4]")
    str(null) should equal(expectedNull)
  }

  test("reverse function test") {
    def reverse(x: Any) = ReverseFunction(Literal(x)).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    reverse("Foo") should equal("ooF")
    reverse("") should equal("")
    reverse(" L") should equal("L ")
    reverse(null) should equal(expectedNull)
    reverse("\r\n") should equal("\n\r")
    reverse("\uD801\uDC37") should equal("\uD801\uDC37")
  }
}
