/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.cypher.internal.ExecutionContext
import values.{ResolvedLabel, LabelName}
import org.neo4j.cypher.internal.pipes.{QueryStateHelper, QueryState}

class StringFunctionsTest extends Assertions {
  @Test def replaceTests() {
    def replace(orig: Any, from: Any, to: Any) =
      ReplaceFunction(Literal(orig), Literal(from), Literal(to)).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(replace("hello", "l", "w") === "hewwo")
    assert(replace("hello", "ell", "ipp") === "hippo")
    assert(replace("hello", "a", "x") === "hello")
    assert(replace(null, "a", "x") === null)
    assert(replace("hello", null, "x") === null)
    assert(replace("hello", "o", null) === null)
    intercept[CypherTypeException](replace(1042, "10", "30"))
  }

  @Test def leftTests() {
    def left(from: Any, r: Any) =
      LeftFunction(Literal(from), Literal(r)).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(left("hello", 2) === "he")
    assert(left("hello", 4) === "hell")
    assert(left("hello", 8) === "hello")
    assert(left(null, 8) === null)
    intercept[CypherTypeException](left(1042, 2))
    intercept[StringIndexOutOfBoundsException](left("hello", -4))
  }

  @Test def rightTests() {
    def right(from: Any, r: Any) =
      RightFunction(Literal(from), Literal(r)).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(right("hello", 2) === "lo")
    assert(right("hello", 4) === "ello")
    assert(right("hello", 8) === "hello")
    assert(right(null, 8) === null)
    intercept[CypherTypeException](right(1024, 2))
    intercept[StringIndexOutOfBoundsException](right("hello", -4))
  }

  @Test def substringTests() {
    def substring(orig: Any, from: Any, to: Any) =
      SubstringFunction(Literal(orig), Literal(from), Some(Literal(to))).apply(ExecutionContext.empty)(QueryStateHelper.empty)
    def substringFrom(orig: Any, from: Any) =
      SubstringFunction(Literal(orig), Literal(from), None).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(substring("hello", 2, 5) === "llo")
    assert(substring("hello", 4, 5) === "o")
    assert(substring("hello", 1, 3) === "ell")
    assert(substring("hello", 8, 5) === "")
    assert(substringFrom("0123456789", 1) === "123456789")
    assert(substringFrom("0123456789", 5) === "56789")
    assert(substringFrom("0123456789", 15) === "")
    assert(substring(null, 8, 5) === null)
    intercept[CypherTypeException](assert(substring(1024, 1, 2) === null))
    intercept[StringIndexOutOfBoundsException](assert(substring("hello", -4, 2) === null))
  }

  @Test def lowerTests() {
    def lower(x: Any) = LowerFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(lower("HELLO") === "hello")
    assert(lower("Hello") === "hello")
    assert(lower("hello") === "hello")
    assert(lower(null) === null)
    intercept[CypherTypeException](lower(1024) === null)
  }

  @Test def upperTests() {
    def upper(x: Any) = UpperFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(upper("HELLO") === "HELLO")
    assert(upper("Hello") === "HELLO")
    assert(upper("hello") === "HELLO")
    assert(upper(null) === null)
    intercept[CypherTypeException](upper(1024) === null)
  }

  @Test def ltrimTests() {
    def ltrim(x: Any) = LTrimFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(ltrim("  HELLO") === "HELLO")
    assert(ltrim(" Hello") === "Hello")
    assert(ltrim("  hello") === "hello")
    assert(ltrim("  hello  ") === "hello  ")
    assert(ltrim(null) === null)
    intercept[CypherTypeException](ltrim(1024))
  }

  @Test def rtrimTests() {
    def rtrim(x: Any) = RTrimFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(rtrim("HELLO  ") === "HELLO")
    assert(rtrim("Hello   ") === "Hello")
    assert(rtrim("  hello   ") === "  hello")
    assert(rtrim(null) === null)
    intercept[CypherTypeException](rtrim(1024))
  }

  @Test def trimTests() {
    def trim(x: Any) = TrimFunction(Literal(x))(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(trim("  hello  ") === "hello")
    assert(trim("  hello ") === "hello")
    assert(trim("hello  ") === "hello")
    assert(trim("  hello  ") === "hello")
    assert(trim("  hello") === "hello")
    assert(trim(null) === null)
    intercept[CypherTypeException](trim(1042))
  }

  @Test def stringTests() {
    def str(x: Any) = StrFunction(Literal(x)).apply(ExecutionContext.empty)(QueryStateHelper.empty)

    assert(str(1234) === "1234")
    assert(str(List(1, 2, 3, 4)) === "[1,2,3,4]")
    assert(str(null) === null)
  }
}
