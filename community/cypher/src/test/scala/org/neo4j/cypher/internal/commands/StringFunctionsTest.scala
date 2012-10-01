/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import expressions._
import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.CypherTypeException

class StringFunctionsTest extends Assertions {
  @Test def replaceTests() {
    assert(ReplaceFunction(Literal("hello"), Literal("l"), Literal("w"))(Map()) === "hewwo")
    assert(ReplaceFunction(Literal("hello"), Literal("ell"), Literal("ipp"))(Map()) === "hippo")
    assert(ReplaceFunction(Literal("hello"), Literal("a"), Literal("x"))(Map()) === "hello")
    assert(ReplaceFunction(Literal(null), Literal("a"), Literal("x"))(Map()) === null)
    intercept[CypherTypeException](ReplaceFunction(Literal(1042), Literal("10"), Literal("30"))(Map()))
  }

  @Test def leftTests() {
    assert(LeftFunction(Literal("hello"), Literal(2))(Map()) === "he")
    assert(LeftFunction(Literal("hello"), Literal(4))(Map()) === "hell")
    assert(LeftFunction(Literal("hello"), Literal(8))(Map()) === "hello")
    assert(LeftFunction(Literal(null), Literal(8))(Map()) === null)
    intercept[CypherTypeException](LeftFunction(Literal(1042), Literal(2))(Map()))
    intercept[StringIndexOutOfBoundsException](LeftFunction(Literal("hello"), Literal(-4))(Map()))
  }

  @Test def rightTests() {
    assert(RightFunction(Literal("hello"), Literal(2))(Map()) === "lo")
    assert(RightFunction(Literal("hello"), Literal(4))(Map()) === "ello")
    assert(RightFunction(Literal("hello"), Literal(8))(Map()) === "hello")
    assert(RightFunction(Literal(null), Literal(8))(Map()) === null)
    intercept[CypherTypeException](RightFunction(Literal(1042), Literal(2))(Map()))
    intercept[StringIndexOutOfBoundsException](RightFunction(Literal("hello"), Literal(-4))(Map()))
  }

  @Test def substringTests() {
    assert(SubstringFunction(Literal("hello"), Literal(2), Literal(5))(Map()) === "llo")
    assert(SubstringFunction(Literal("hello"), Literal(4), Literal(5))(Map()) === "o")
    assert(SubstringFunction(Literal("hello"), Literal(1), Literal(3))(Map()) === "ell")
    assert(SubstringFunction(Literal("hello"), Literal(8), Literal(5))(Map()) === "")
    assert(SubstringFunction(Literal(null), Literal(8), Literal(5))(Map()) === null)
    intercept[CypherTypeException](SubstringFunction(Literal(1042), Literal(1), Literal(2))(Map()))
    intercept[StringIndexOutOfBoundsException](SubstringFunction(Literal("hello"), Literal(-4), Literal(5))(Map()))
  }

  @Test def lowerTests() {
    assert(LowerFunction(Literal("HELLO"))(Map()) === "hello")
    assert(LowerFunction(Literal("Hello"))(Map()) === "hello")
    assert(LowerFunction(Literal("hello"))(Map()) === "hello")
    assert(LowerFunction(Literal(null))(Map()) === null)
    intercept[CypherTypeException](LowerFunction(Literal(1042))(Map()))
  }

  @Test def upperTests() {
    assert(UpperFunction(Literal("HELLO"))(Map()) === "HELLO")
    assert(UpperFunction(Literal("Hello"))(Map()) === "HELLO")
    assert(UpperFunction(Literal("hello"))(Map()) === "HELLO")
    assert(UpperFunction(Literal(null))(Map()) === null)
    intercept[CypherTypeException](UpperFunction(Literal(1042))(Map()))
  }

  @Test def ltrimTests() {
    assert(LTrimFunction(Literal("  HELLO"))(Map()) === "HELLO")
    assert(LTrimFunction(Literal(" Hello"))(Map()) === "Hello")
    assert(LTrimFunction(Literal("  hello"))(Map()) === "hello")
    assert(LTrimFunction(Literal("  hello  "))(Map()) === "hello  ")
    assert(LTrimFunction(Literal(null))(Map()) === null)
    intercept[CypherTypeException](LTrimFunction(Literal(1042))(Map()))
  }

  @Test def rtrimTests() {
    assert(RTrimFunction(Literal("HELLO  "))(Map()) === "HELLO")
    assert(RTrimFunction(Literal("Hello   "))(Map()) === "Hello")
    assert(RTrimFunction(Literal("  hello  "))(Map()) === "  hello")
    assert(RTrimFunction(Literal(null))(Map()) === null)
    intercept[CypherTypeException](RTrimFunction(Literal(1042))(Map()))
  }

  @Test def trimTests() {
    assert(TrimFunction(Literal("  hello  "))(Map()) === "hello")
    assert(TrimFunction(Literal("  hello "))(Map()) === "hello")
    assert(TrimFunction(Literal("hello  "))(Map()) === "hello")
    assert(TrimFunction(Literal("  hello  "))(Map()) === "hello")
    assert(TrimFunction(Literal("  hello"))(Map()) === "hello")
    assert(TrimFunction(Literal(null))(Map()) === null)
    intercept[CypherTypeException](TrimFunction(Literal(1042))(Map()))
  }

  @Test def stringTests() {
    assert(StrFunction(Literal(1234))(Map()) === "1234")
    assert(StrFunction(Literal(List(1,2,3,4)))(Map()) === "[1,2,3,4]")
    assert(StrFunction(Literal(null))(Map()) === null)
  }
}
