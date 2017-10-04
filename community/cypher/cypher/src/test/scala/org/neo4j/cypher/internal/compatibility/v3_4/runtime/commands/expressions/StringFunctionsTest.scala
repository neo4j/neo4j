/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.{EMPTY_STRING, stringValue}

class StringFunctionsTest extends CypherFunSuite {

  // TODO Move these tests into individual classes, at least for the more complex ones

  private val expectedNull = Values.NO_VALUE

  test("replaceTests") {
    def replace(orig: Any, from: Any, to: Any) =
      ReplaceFunction(Literal(orig), Literal(from), Literal(to)).apply(ExecutionContext.empty, QueryStateHelper.empty)

    replace("hello", "l", "w") should equal(stringValue("hewwo"))
    replace("hello", "ell", "ipp") should equal(stringValue("hippo"))
    replace("hello", "a", "x") should equal(stringValue("hello"))
    replace(null, "a", "x") should equal(expectedNull)
    replace("hello", null, "x") should equal(expectedNull)
    replace("hello", "o", null) should equal(expectedNull)
    intercept[CypherTypeException](replace(1042, "10", "30"))
  }

  test("leftTests") {
    def left(from: Any, r: Any) =
      LeftFunction(Literal(from), Literal(r)).apply(ExecutionContext.empty, QueryStateHelper.empty)

    left("hello", 2) should equal(stringValue("he"))
    left("hello", 4) should equal(stringValue("hell"))
    left("hello", 8) should equal(stringValue("hello"))
    left(null, 8) should equal(expectedNull)
    intercept[CypherTypeException](left(1042, 2))
    intercept[StringIndexOutOfBoundsException](left("hello", -4))
  }

  test("rightTests") {
    def right(from: Any, r: Any) =
      RightFunction(Literal(from), Literal(r)).apply(ExecutionContext.empty, QueryStateHelper.empty)

    right("hello", 2) should equal(stringValue("lo"))
    right("hello", 4) should equal(stringValue("ello"))
    right("hello", 8) should equal(stringValue("hello"))
    right(null, 8) should equal(expectedNull)
    intercept[CypherTypeException](right(1024, 2))
    intercept[StringIndexOutOfBoundsException](right("hello", -4))
  }

  test("substringTests") {
    def substring(orig: Any, from: Any, to: Any) = {
      val function = SubstringFunction(Literal(orig), Literal(from), Some(Literal(to)))
      function.apply(ExecutionContext.empty, QueryStateHelper.empty)
    }
    def substringFrom(orig: Any, from: Any) = {
      val function = SubstringFunction(Literal(orig), Literal(from), None)
      function.apply(ExecutionContext.empty, QueryStateHelper.empty)
    }

    substring("hello", 2, 5) should equal(stringValue("llo"))
    substring("hello", 4, 5) should equal(stringValue("o"))
    substring("hello", 1, 3) should equal(stringValue("ell"))
    substring("hello", 8, 5) should equal(EMPTY_STRING)
    substringFrom("0123456789", 1) should equal(stringValue("123456789"))
    substringFrom("0123456789", 5) should equal(stringValue("56789"))
    substringFrom("0123456789", 15) should equal(EMPTY_STRING)
    substring(null, 8, 5) should equal(expectedNull)
    intercept[CypherTypeException](substring(1024, 1, 2) should equal(expectedNull))
    intercept[StringIndexOutOfBoundsException](substring("hello", -4, 2) should equal(expectedNull))
  }

  test("lowerTests") {
    def lower(x: Any) = ToLowerFunction(Literal(x))(ExecutionContext.empty, QueryStateHelper.empty)

    lower("HELLO") should equal(stringValue("hello"))
    lower("Hello") should equal(stringValue("hello"))
    lower("hello") should equal(stringValue("hello"))
    lower(null) should equal(expectedNull)
    intercept[CypherTypeException](lower(1024) should equal(expectedNull))
  }

  test("upperTests") {
    def upper(x: Any) = ToUpperFunction(Literal(x))(ExecutionContext.empty, QueryStateHelper.empty)

    upper("HELLO") should equal(stringValue("HELLO"))
    upper("Hello") should equal(stringValue("HELLO"))
    upper("hello") should equal(stringValue("HELLO"))
    upper(null) should equal(expectedNull)
    intercept[CypherTypeException](upper(1024) should equal(expectedNull))
  }

  test("ltrimTests") {
    def ltrim(x: Any) = LTrimFunction(Literal(x))(ExecutionContext.empty, QueryStateHelper.empty)

    ltrim("  HELLO") should equal(stringValue("HELLO"))
    ltrim(" Hello") should equal(stringValue("Hello"))
    ltrim("  hello") should equal(stringValue("hello"))
    ltrim("  hello  ") should equal(stringValue("hello  "))
    ltrim(null) should equal(expectedNull)
    intercept[CypherTypeException](ltrim(1024))
  }

  test("rtrimTests") {
    def rtrim(x: Any) = RTrimFunction(Literal(x))(ExecutionContext.empty, QueryStateHelper.empty)

    rtrim("HELLO  ") should equal(stringValue("HELLO"))
    rtrim("Hello   ") should equal(stringValue("Hello"))
    rtrim("  hello   ") should equal(stringValue("  hello"))
    rtrim(null) should equal(expectedNull)
    intercept[CypherTypeException](rtrim(1024))
  }

  test("trimTests") {
    def trim(x: Any) = TrimFunction(Literal(x))(ExecutionContext.empty, QueryStateHelper.empty)

    trim("  hello  ") should equal(stringValue("hello"))
    trim("  hello ") should equal(stringValue("hello"))
    trim("hello  ") should equal(stringValue("hello"))
    trim("  hello  ") should equal(stringValue("hello"))
    trim("  hello") should equal(stringValue("hello"))
    trim(null) should equal(expectedNull)
    intercept[CypherTypeException](trim(1042))
  }

  test("reverse function test") {
    def reverse(x: Any) = ReverseFunction(Literal(x)).apply(ExecutionContext.empty, QueryStateHelper.empty)

    reverse("Foo") should equal(stringValue("ooF"))
    reverse("") should equal(EMPTY_STRING)
    reverse(" L") should equal(stringValue("L "))
    reverse(null) should equal(expectedNull)
    reverse("\r\n") should equal(stringValue("\n\r"))
    reverse("\uD801\uDC37") should equal(stringValue("\uD801\uDC37"))
  }
}
