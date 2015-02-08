/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.prettifier

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.parser.ParserTest

class PrettifierParserTest extends PrettifierParser with ParserTest[Seq[SyntaxToken], Seq[SyntaxToken]] {

  implicit val parserToTest = main

  def convert(values: Seq[SyntaxToken]): Seq[SyntaxToken] = values

  @Test
  def shouldParseKeywords() {
    // given
    val keyword = "create"

    // when then
    parsing(keyword) shouldGive
      Seq(BreakingKeywords(keyword))
  }


  @Test
  def shouldNotParseAssertAsANonBreakingKeyword() {
    // given
    val query = "create constraint on (person:Person) assert person.age is unique"

    // when then
    parsing(query) shouldGive
      Seq(BreakingKeywords("create constraint on"), OpenGroup("("), AnyText("person:Person"), CloseGroup(")"),
        NonBreakingKeywords("assert"), AnyText("person.age"), NonBreakingKeywords("is unique"))
  }

  @Test
  def shouldParseIndexAsKeyword() {
    // given
    val keyword = "index"

    // when then
    parsing(keyword) shouldGive
      Seq(NonBreakingKeywords(keyword))
  }

  @Test
  def shouldParseAnyText() {
    // given
    val input = "a-->b"

    // when then
    parsing(input) shouldGive
      Seq(AnyText(input))
  }

  @Test
  def shouldParseEscapedText() {
    // given
    val input = "aha!"

    // when then
    parsing("\"" + input + "\"") shouldGive
      Seq(EscapedText(input))
  }

  @Test
  def shouldParseGroupingText() {
    // given
    val input = "(}{)[]"

    // when then
    parsing(input) shouldGive
      Seq(OpenGroup("("),
          CloseGroup("}"),
          OpenGroup("{"),
          CloseGroup(")"),
          OpenGroup("["),
          CloseGroup("]"))
  }

  @Test
  def shouldParseComplexExample1() {
    // given
    val input = "match a-->b where b.name = \"aha!\" return a.age"

    // when then
    parsing(input) shouldGive
      Seq(BreakingKeywords("match"), AnyText("a-->b"), BreakingKeywords("where"), AnyText("b.name"), AnyText("="),
          EscapedText("aha!"), BreakingKeywords("return"), AnyText("a.age"))
  }

  @Test
  def shouldParseComplexExample2() {
    // given
    val input = "merge n on create set n.age=32"

    // when then
    parsing(input) shouldGive
      Seq(
        BreakingKeywords("merge"), AnyText("n"),
        BreakingKeywords("on create"), BreakingKeywords("set"), AnyText("n.age=32"))
  }

  @Test
  def shouldParseComplexGrouping() {
    val result = parsing("[(0,10)]")
    result shouldGive Seq(OpenGroup("["),OpenGroup("("), AnyText("0,10"), CloseGroup(")"), CloseGroup("]"))
  }

  @Test
  def shouldParseGroupingWithEscapedText() {
    val result = parsing("( \"Gunhild\" )")
    result shouldGive Seq(OpenGroup("("), EscapedText("Gunhild"), CloseGroup(")"))
  }


  @Test
  def shouldParseGrouping() {
    parsing("(x)") shouldGive Seq(OpenGroup("("), AnyText("x"), CloseGroup(")"))
    parsing("[x]") shouldGive Seq(OpenGroup("["), AnyText("x"), CloseGroup("]"))
    parsing("{x}") shouldGive Seq(OpenGroup("{"), AnyText("x"), CloseGroup("}"))
  }

  @Test
  def shouldParseStringsAndKeepQuotes() {
    parsing("\"I'm a literal\"") shouldGive Seq(EscapedText("I'm a literal"))
    parsing("'Im a literal'") shouldGive Seq(EscapedText("Im a literal", '\''))
    parsing("'I\\'m a literal'") shouldGive Seq(EscapedText("I\'m a literal", '\''))
  }
}
