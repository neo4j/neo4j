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
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue

class StringRegexReplaceFunctionTest extends InterpretedRuntimeTestSuite {

  test("passing null to string.regexReplace() returns null") {
    regexReplace(null, "a", "b") should be(NO_VALUE)
    regexReplace("a", null, "b") should be(NO_VALUE)
    regexReplace("a", "a", null) should be(NO_VALUE)
    regexReplace(null, null, null) should be(NO_VALUE)
  }

  test("string.regexReplace() should replace matches") {
    regexReplace("hello", "l", "w") should be(stringValue("hewwo"))
    regexReplace("hello", "ell", "ipp") should be(stringValue("hippo"))
    regexReplace("123-456-789", "\\d", "x") should be(stringValue("xxx-xxx-xxx"))
  }

  test("string.regexReplace() with capturing groups") {
    regexReplace("abc-def", "(\\w+)-(\\w+)", "$2-$1") should be(stringValue("def-abc"))
  }

  test("string.regexReplace() with no matches") {
    regexReplace("hello", "x", "y") should be(stringValue("hello"))
  }

  test("string.regexReplace() with invalid regex") {
    intercept[InvalidSemanticsException] {
      regexReplace("hello", "[", "y")
    }.getMessage should include("Invalid Regex: Unclosed character class")
  }

  test("string.regexReplace() should throw CypherTypeException for non-string input") {
    intercept[CypherTypeException] {
      regexReplace(1, "a", "b")
    }.getMessage should include("Expected a string value for `string.regexReplace`, but got: Int(1)")

    intercept[CypherTypeException] {
      regexReplace("a", 1, "b")
    }.getMessage should include("Expected a string value for `string.regexReplace`, but got: Int(1)")

    intercept[CypherTypeException] {
      regexReplace("a", "a", 1)
    }.getMessage should include("Expected a string value for `string.regexReplace`, but got: Int(1)")
  }

  test("LiteralStringRegexReplaceFunction compiles pattern once") {
    val expr = LiteralStringRegexReplaceFunction(literal("hello"), literal("l"), literal("w"))
    expr.pattern should be theSameInstanceAs expr.pattern
    expr(CypherRow.empty, QueryStateHelper.empty) should be(stringValue("hewwo"))
  }

  test("LiteralStringRegexReplaceFunction handles null input and replacement") {
    val regexLit = literal("l")
    LiteralStringRegexReplaceFunction(Literal(NO_VALUE), regexLit, literal("w"))(
      CypherRow.empty,
      QueryStateHelper.empty
    ) should be(NO_VALUE)
    LiteralStringRegexReplaceFunction(literal("hello"), regexLit, Literal(NO_VALUE))(
      CypherRow.empty,
      QueryStateHelper.empty
    ) should be(NO_VALUE)
  }

  test("StringRegexReplaceFunction.rewrite promotes literal regex to LiteralStringRegexReplaceFunction") {
    val expr = StringRegexReplaceFunction(literal("hello"), literal("l"), literal("w"))
    expr.rewrite(identity) shouldBe a[LiteralStringRegexReplaceFunction]
  }

  private def regexReplace(input: Any, regex: Any, replacement: Any) = {
    val expr = StringRegexReplaceFunction(literal(input), literal(regex), literal(replacement))
    expr(CypherRow.empty, QueryStateHelper.empty)
  }
}
