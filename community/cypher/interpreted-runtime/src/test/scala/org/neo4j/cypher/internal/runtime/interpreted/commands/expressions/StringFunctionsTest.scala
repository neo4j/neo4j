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
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.values.storable.UTF8StringValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.EMPTY_STRING
import org.neo4j.values.storable.Values.stringArray
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.storable.Values.utf8Value

import java.nio.charset.StandardCharsets

class StringFunctionsTest extends CypherFunSuite {

  // TODO Move these tests into individual classes, at least for the more complex ones

  private val expectedNull = Values.NO_VALUE

  test("replaceTests") {
    def replace(orig: Any, from: Any, to: Any) =
      ReplaceFunction(literal(orig), literal(from), literal(to)).apply(CypherRow.empty, QueryStateHelper.empty)

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
      LeftFunction(literal(from), literal(r)).apply(CypherRow.empty, QueryStateHelper.empty)

    left("hello", 2) should equal(stringValue("he"))
    left("hello", 4) should equal(stringValue("hell"))
    left("hello", 8) should equal(stringValue("hello"))
    left(null, 8) should equal(expectedNull)
    intercept[CypherTypeException](left(1042, 2))
    intercept[IndexOutOfBoundsException](left("hello", -4))
  }

  test("rightTests") {
    def right(from: Any, r: Any) =
      RightFunction(literal(from), literal(r)).apply(CypherRow.empty, QueryStateHelper.empty)

    right("hello", 2) should equal(stringValue("lo"))
    right("hello", 4) should equal(stringValue("ello"))
    right("hello", 8) should equal(stringValue("hello"))
    right(null, 8) should equal(expectedNull)
    intercept[CypherTypeException](right(1024, 2))
    intercept[IndexOutOfBoundsException](right("hello", -4))
  }

  test("substringTests") {
    def substring(orig: Any, from: Any, to: Any) = {
      val function = SubstringFunction(literal(orig), literal(from), Some(literal(to)))
      function.apply(CypherRow.empty, QueryStateHelper.empty)
    }
    def substringFrom(orig: Any, from: Any) = {
      val function = SubstringFunction(literal(orig), literal(from), None)
      function.apply(CypherRow.empty, QueryStateHelper.empty)
    }

    substring("a", 2, Int.MaxValue) should equal(EMPTY_STRING)
    substring("hello", 2, 5) should equal(stringValue("llo"))
    substring("hello", 4, 5) should equal(stringValue("o"))
    substring("hello", 1, 3) should equal(stringValue("ell"))
    substring("hello", 8, 5) should equal(EMPTY_STRING)
    substringFrom("0123456789", 1) should equal(stringValue("123456789"))
    substringFrom("0123456789", 5) should equal(stringValue("56789"))
    substringFrom("0123456789", 15) should equal(EMPTY_STRING)
    substring(null, 8, 5) should equal(expectedNull)
    substring("\uD83D\uDE21\uD83D\uDE21\uD83D\uDE21", 1, 1) should equal(stringValue("\uD83D\uDE21"))
    substring("\uD83D\uDE21\uD83D\uDE21\uD83D\uDE21", 1, 2) should equal(stringValue("\uD83D\uDE21\uD83D\uDE21"))
    intercept[CypherTypeException](substring(1024, 1, 2) should equal(expectedNull))
    intercept[IndexOutOfBoundsException](substring("hello", -4, 2) should equal(expectedNull))
  }

  test("lowerTests") {
    def lower(x: Any) = ToLowerFunction(literal(x))(CypherRow.empty, QueryStateHelper.empty)

    lower("HELLO") should equal(stringValue("hello"))
    lower("Hello") should equal(stringValue("hello"))
    lower("hello") should equal(stringValue("hello"))
    lower(null) should equal(expectedNull)
    intercept[CypherTypeException](lower(1024) should equal(expectedNull))
  }

  test("upperTests") {
    def upper(x: Any) = ToUpperFunction(literal(x))(CypherRow.empty, QueryStateHelper.empty)

    upper("HELLO") should equal(stringValue("HELLO"))
    upper("Hello") should equal(stringValue("HELLO"))
    upper("hello") should equal(stringValue("HELLO"))
    upper(null) should equal(expectedNull)
    intercept[CypherTypeException](upper(1024) should equal(expectedNull))
  }

  test("ltrimTests") {
    def ltrim(x: Any) = LTrimFunction(literal(x))(CypherRow.empty, QueryStateHelper.empty)
    def ltrimWithTrimString(x: Any, y: Any) =
      LTrimFunction(literal(x), Some(literal(y)))(CypherRow.empty, QueryStateHelper.empty)

    ltrim("  HELLO") should equal(stringValue("HELLO"))
    ltrimWithTrimString("  HELLO", " ") should equal(stringValue("HELLO"))
    ltrim(" Hello") should equal(stringValue("Hello"))
    ltrim("  hello") should equal(stringValue("hello"))
    ltrimWithTrimString("  hello", " ") should equal(stringValue("hello"))
    ltrim("  hello  ") should equal(stringValue("hello  "))
    ltrimWithTrimString("  hello  ", " ") should equal(stringValue("hello  "))
    ltrimWithTrimString("xyxyxrxxxxhelloxxx", "xy") should equal(stringValue("rxxxxhelloxxx"))
    ltrim(null) should equal(expectedNull)
    ltrimWithTrimString(null, " ") should equal(expectedNull)
    ltrimWithTrimString(null, null) should equal(expectedNull)
    ltrimWithTrimString("  HELLO", null) should equal(expectedNull)
    ltrim("\u2009㺂࿝鋦毠\u2009") should equal(stringValue("㺂࿝鋦毠\u2009")) // Contains `thin space`
    ltrimWithTrimString("\u2009㺂࿝鋦毠\u2009", "\u2009") should equal(stringValue("㺂࿝鋦毠\u2009")) // Contains `thin space`
    intercept[CypherTypeException](ltrim(1024))
    intercept[CypherTypeException](ltrimWithTrimString(1024, "1"))
  }

  test("rtrimTests") {
    def rtrim(x: Any) = RTrimFunction(literal(x))(CypherRow.empty, QueryStateHelper.empty)
    def rtrimWithTrimString(x: Any, y: Any) =
      RTrimFunction(literal(x), Some(literal(y)))(CypherRow.empty, QueryStateHelper.empty)

    rtrim("HELLO  ") should equal(stringValue("HELLO"))
    rtrimWithTrimString("HELLO  ", " ") should equal(stringValue("HELLO"))
    rtrim("Hello   ") should equal(stringValue("Hello"))
    rtrimWithTrimString("Hello   ", " ") should equal(stringValue("Hello"))
    rtrim("  hello   ") should equal(stringValue("  hello"))
    rtrimWithTrimString("  hello   ", " ") should equal(stringValue("  hello"))
    rtrimWithTrimString("xyxyxhelloyyyxxrxyxyx", "xy") should equal(stringValue("xyxyxhelloyyyxxr"))
    rtrim(null) should equal(expectedNull)
    rtrimWithTrimString(null, " ") should equal(expectedNull)
    rtrimWithTrimString(null, null) should equal(expectedNull)
    rtrimWithTrimString("hi", null) should equal(expectedNull)
    rtrim("\u2009㺂࿝鋦毠\u2009") should equal(stringValue("\u2009㺂࿝鋦毠")) // Contains `thin space`
    rtrimWithTrimString("\u2009㺂࿝鋦毠\u2009", "\u2009") should equal(stringValue("\u2009㺂࿝鋦毠")) // Contains `thin space`
    intercept[CypherTypeException](rtrim(1024))
    intercept[CypherTypeException](rtrimWithTrimString(1024, "1"))
  }

  test("btrimTests") {
    def btrim(x: Any) = BTrimFunction(literal(x))(CypherRow.empty, QueryStateHelper.empty)
    def btrimWithTrimString(x: Any, y: Any) =
      BTrimFunction(literal(x), Some(literal(y)))(CypherRow.empty, QueryStateHelper.empty)

    btrim("  hello  ") should equal(stringValue("hello"))
    btrimWithTrimString("  hello  ", " ") should equal(stringValue("hello"))
    btrim("  hello ") should equal(stringValue("hello"))
    btrim("hello  ") should equal(stringValue("hello"))
    btrimWithTrimString("hello  ", " ") should equal(stringValue("hello"))
    btrim("  hello  ") should equal(stringValue("hello"))
    btrim("  hello") should equal(stringValue("hello"))
    btrimWithTrimString("  hello", " ") should equal(stringValue("hello"))
    btrimWithTrimString("xyxyxrxxyhelloyyyxxrxyxyx", "xy") should equal(stringValue("rxxyhelloyyyxxr"))
    btrim("\u2009㺂࿝鋦毠\u2009") should equal(stringValue("㺂࿝鋦毠")) // Contains `thin space`
    btrimWithTrimString("\u2009㺂࿝鋦毠\u2009", "\u2009") should equal(stringValue("㺂࿝鋦毠")) // Contains `thin space`
    btrim(null) should equal(expectedNull)
    btrimWithTrimString(null, " ") should equal(expectedNull)
    btrimWithTrimString(null, null) should equal(expectedNull)
    btrimWithTrimString("hi", null) should equal(expectedNull)
    intercept[CypherTypeException](btrim(1042))
    intercept[CypherTypeException](btrimWithTrimString(1042, "1"))
  }

  test("trimTests") {
    def trim(x: Any) = TrimFunction(literal("BOTH"), literal(x))(CypherRow.empty, QueryStateHelper.empty)

    trim("  hello  ") should equal(stringValue("hello"))
    trim("  hello ") should equal(stringValue("hello"))
    trim("hello  ") should equal(stringValue("hello"))
    trim("  hello  ") should equal(stringValue("hello"))
    trim("  hello") should equal(stringValue("hello"))
    trim("\u2009㺂࿝鋦毠\u2009") should equal(stringValue("㺂࿝鋦毠")) // Contains `thin space`
    trim(null) should equal(expectedNull)
    intercept[CypherTypeException](trim(1042))

    trim("  hello  ") should equal(stringValue("hello"))

    // Trim with utf8Values
    val source = "  helloworld  ".getBytes(StandardCharsets.UTF_8)
    val trimCharString = " ".getBytes(StandardCharsets.UTF_8)
    val state = QueryStateHelper.emptyWith(params = Array(utf8Value(source), utf8Value(trimCharString)))
    val result = TrimFunction(literal("BOTH"), ParameterFromSlot(0, "p1"), Some(ParameterFromSlot(1, "p2")))(
      CypherRow.empty,
      state
    )
    // Then
    result shouldBe a[UTF8StringValue]
    result should equal(utf8Value("helloworld".getBytes(StandardCharsets.UTF_8)))
    result should equal(Values.stringValue("helloworld"))
  }

  test("reverse function test") {
    def reverse(x: Any) = ReverseFunction(literal(x)).apply(CypherRow.empty, QueryStateHelper.empty)

    reverse("Foo") should equal(stringValue("ooF"))
    reverse("") should equal(EMPTY_STRING)
    reverse(" L") should equal(stringValue("L "))
    reverse(null) should equal(expectedNull)
    reverse("\r\n") should equal(stringValue("\n\r"))
    reverse("\uD801\uDC37") should equal(stringValue("\uD801\uDC37"))
  }

  test("splitTests") {
    def split(x: Any, y: Any) = SplitFunction(literal(x), literal(y))(CypherRow.empty, QueryStateHelper.empty)

    split("HELLO", "LL") should equal(stringArray("HE", "O"))
    split("Separating,by,comma,is,a,common,use,case", ",") should equal(stringArray(
      "Separating",
      "by",
      "comma",
      "is",
      "a",
      "common",
      "use",
      "case"
    ))
    split("hello", "X") should equal(stringArray("hello"))
    split("hello", null) should equal(expectedNull)
    split(null, "hello") should equal(expectedNull)
    split(null, null) should equal(expectedNull)
    split("Hello", "") should equal(stringArray("H", "e", "l", "l", "o"))
    split("joe@soap.com", Seq("@", ".")) should equal(stringArray("joe", "soap", "com"))
    intercept[CypherTypeException](split(1024, 10))
  }

  test("normalizeTests") {
    def normalize(x: Any, y: Any) = NormalizeFunction(literal(x), literal(y))(CypherRow.empty, QueryStateHelper.empty)

    normalize("\u212B", "NFC") should equal(stringValue("\u00C5"))
    normalize("\u00E4", "NFD") should equal(stringValue("\u0061\u0308"))
    normalize("\uFE64script\uFE65", "NFKC") should equal(stringValue("\u003Cscript\u003E"))
    normalize("\u003Cscript\u003E", "NFKD") should equal(stringValue("\u003Cscript\u003E"))
    normalize("hello", null) should equal(expectedNull)
    normalize(null, "hello") should equal(expectedNull)
    normalize(null, null) should equal(expectedNull)
    intercept[CypherTypeException](normalize(1024, 10))
    intercept[InvalidArgumentException](normalize("HELLO", "WORLD"))
  }
}
