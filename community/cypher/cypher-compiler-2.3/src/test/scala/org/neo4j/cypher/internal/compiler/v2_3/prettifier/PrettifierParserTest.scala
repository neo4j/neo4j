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
package org.neo4j.cypher.internal.compiler.v2_3.prettifier

import org.neo4j.cypher.internal.frontend.v2_3.parser.ParserTest

class PrettifierParserTest extends ParserTest[Seq[SyntaxToken], Seq[SyntaxToken]] {

  implicit val parserToTest = new PrettifierParser().main

  def convert(values: Seq[SyntaxToken]): Seq[SyntaxToken] = values

  test("shouldParseKeywords") {
    // given
    val keyword = "create"

    // when then
    parsing(keyword) shouldGive
      Seq(BreakingKeywords(keyword))
  }

  test("shouldNotParseAssertAsANonBreakingKeyword") {
    // given
    val query = "create constraint on (person:Person) assert person.age is unique"

    // when then
    parsing(query) shouldGive
      Seq(BreakingKeywords("create constraint on"), GroupToken("(", ")", Seq(AnyText("person:Person"))),
        NonBreakingKeywords("assert"), AnyText("person.age"), NonBreakingKeywords("is unique"))
  }

  test("shouldParseIndexAsKeyword") {
    // given
    val keyword = "asc"

    // when then
    parsing(keyword) shouldGive
      Seq(NonBreakingKeywords(keyword))
  }

  test("shouldParseAnyText") {
    // given
    val input = "a-->b"

    // when then
    parsing(input) shouldGive
      Seq(AnyText(input))
  }

  test("shouldParseEscapedText") {
    // given
    val input = "aha!"

    // when then
    parsing("\"" + input + "\"") shouldGive
      Seq(EscapedText(input))
  }

  test("shouldParseGroupingText") {
    // given
    val input = "(){}[]"

    // when then
    parsing(input) shouldGive
      Seq(
        GroupToken("(", ")", Seq.empty),
        GroupToken("{", "}", Seq.empty),
        GroupToken("[", "]", Seq.empty)
      )
  }

  test("shouldParseComplexExample1") {
    // given
    val input = "match a-->b where b.name = \"aha!\" return a.age"

    // when then
    parsing(input) shouldGive
      Seq(BreakingKeywords("match"), AnyText("a-->b"), BreakingKeywords("where"), AnyText("b.name"), AnyText("="),
          EscapedText("aha!"), BreakingKeywords("return"), AnyText("a.age"))
  }

  test("shouldParseComplexExample2") {
    // given
    val input = "merge n on create set n.age=32"

    // when
    val result = parsing(input)

    // then
    val expectation = Seq(
      BreakingKeywords("merge"),
      AnyText("n"),
      BreakingKeywords("on create set"),
      AnyText("n.age=32")
    )
    result shouldGive expectation
  }

  test("shouldParseSimpleGrouping") {
    val result = parsing("[0,10]")
    result shouldGive Seq(GroupToken("[", "]", Seq(AnyText("0,10"))))
  }

  test("shouldParseComplexGrouping") {
    val result = parsing("[(0,10)]")
    result shouldGive Seq(
      GroupToken("[", "]", Seq(
        GroupToken("(", ")", Seq(AnyText("0,10")))
      )
    ))
  }

  test("shouldParseGroupingWithEscapedText") {
    val result = parsing("( \"Gunhild\" )")
    result shouldGive Seq(GroupToken("(", ")", Seq(EscapedText("Gunhild"))))
  }

  test("shouldParseGrouping") {
    parsing("(x)") shouldGive Seq(GroupToken("(", ")", Seq(AnyText("x"))))
    parsing("[x]") shouldGive Seq(GroupToken("[", "]", Seq(AnyText("x"))))
    parsing("{x}") shouldGive Seq(GroupToken("{", "}", Seq(AnyText("x"))))
  }

  test("shouldParseWhereAsNonBreakingInsideGrouping") {
    val result = parsing("( WHERE )")
    result shouldGive Seq(GroupToken("(", ")", Seq(NonBreakingKeywords("WHERE"))))
  }

  test("shouldParseUsingPeriodicCommitAndMatchAsDistinctKeywordGroups") {
    val result = parsing("USING PERIODIC COMMIT MATCH")
    result shouldGive Seq(BreakingKeywords("USING PERIODIC COMMIT"), BreakingKeywords("MATCH"))
  }

  test("shouldParseStringsAndKeepQuotes") {
    parsing("\"I'm a literal\"") shouldGive Seq(EscapedText("I'm a literal"))
    parsing("'Im a literal'") shouldGive Seq(EscapedText("Im a literal", '\''))
    parsing("'I\\'m a literal'") shouldGive Seq(EscapedText("I\'m a literal", '\''))
  }
}
