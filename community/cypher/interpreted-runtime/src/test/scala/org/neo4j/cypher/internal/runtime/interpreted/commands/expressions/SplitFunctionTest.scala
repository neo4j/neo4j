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
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues.list

class SplitFunctionTest extends CypherFunSuite {

  val nullSeq = null.asInstanceOf[Seq[String]]
  val nullString = null.asInstanceOf[String]

  test("passing null to split() returns null") {
    split("something", nullString) should be(NO_VALUE)
    split(nullString, "something") should be(NO_VALUE)
  }

  test("splitting non-empty strings with one character") {
    split("first,second", ",") should be(seq("first", "second"))
  }

  test("splitting non-empty strings with more than one character") {
    split("first11second11third", "11") should be(seq("first", "second", "third"))
  }

  test("splitting an empty string should return an empty string") {
    split("", ",") should be(seq(""))
  }

  test("splitting a string containing only the split pattern should return two empty strings") {
    split(",", ",") should be(seq("", ""))
  }

  test("splitting with character that has a meaning in regex") {
    split("a|b", "|") should be(seq("a", "b"))
  }

  test("using an empty separator should split on every character") {
    split("banana", "") should be(seq("b", "a", "n", "a", "n", "a"))
    split("a", "") should be(seq("a"))
    split("", "") should be(seq(""))
  }

  test("splitting non-empty string with multiple separator characters") {
    split("first,second;third", List(",", ";")) should be(seq("first", "second", "third"))
  }

  test("splitting non-empty string with multiple separator characters as array") {
    split("first,second;third", Array(",", ";")) should be(seq("first", "second", "third"))
  }

  test("splitting non-empty string with multiple separator strings") {
    split("(a)-->(b)<--(c)-->(d)--(e)", List("-->", "<--", "--")) should be(seq("(a)", "(b)", "(c)", "(d)", "(e)"))
  }

  test("splitting non-empty string with multiple separator strings as array") {
    split("(a)-->(b)<--(c)-->(d)--(e)", Array("-->", "<--", "--")) should be(seq("(a)", "(b)", "(c)", "(d)", "(e)"))
  }

  test(
    "splitting non-empty string with multiple separator strings where one is empty should return all one-char substrings"
  ) {
    val expected = list("this is a sentence".split("").map(stringValue): _*)
    split("this is a sentence", List(",", ";", "")) should be(expected)
  }

  test(
    "splitting non-empty string with multiple separator strings where one is empty and others match valid characters should return all one-char substrings without the other matching characters"
  ) {
    val sentence = ";This is a sentence;, with punctuation..."
    val expected = list(sentence.replaceAll("[,.;]", "").split("").map(stringValue): _*)
    split(sentence, List(",", ".", ";", "")) should be(expected)
  }

  test("splitting char with separator set to same char should return empty") {
    split('a', "a") should be(seq("", ""))
  }

  test("splitting char with separator set to different char should return original") {
    split('a', "b") should be(seq("a"))
  }

  test("splitting char with multiple separator characters where one is the same should return empty") {
    split('a', List("a", "b")) should be(seq("", ""))
  }

  private def seq(vals: String*) = list(vals.map(stringValue): _*)

  private def split(orig: String, splitPattern: String) = {
    val expr = SplitFunction(literal(orig), literal(splitPattern))
    expr(CypherRow.empty, QueryStateHelper.empty)
  }

  private def split(orig: Char, splitPattern: String) = {
    val expr = SplitFunction(literal(orig), literal(splitPattern))
    expr(CypherRow.empty, QueryStateHelper.empty)
  }

  private def split(orig: String, splitDelimiters: List[String]) = {
    val expr = SplitFunction(literal(orig), literal(splitDelimiters))
    expr(CypherRow.empty, QueryStateHelper.empty)
  }

  private def split(orig: String, splitDelimiters: Array[String]) = {
    val expr = SplitFunction(literal(orig), literal(splitDelimiters))
    expr(CypherRow.empty, QueryStateHelper.empty)
  }

  private def split(orig: Char, splitDelimiters: List[String]) = {
    val expr = SplitFunction(literal(orig), literal(splitDelimiters))
    expr(CypherRow.empty, QueryStateHelper.empty)
  }
}
