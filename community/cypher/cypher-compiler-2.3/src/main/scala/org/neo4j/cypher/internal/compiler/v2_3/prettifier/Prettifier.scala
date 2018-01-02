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

import org.neo4j.cypher.internal.frontend.v2_3.SyntaxException
import org.neo4j.cypher.internal.frontend.v2_3.parser.{Base, Strings}
import org.parboiled.scala._

import scala.collection.mutable

sealed abstract class SyntaxToken {
  def text: String

  override def toString = text
}

sealed abstract class KeywordToken extends SyntaxToken {
  override def toString = text.toUpperCase
}

final case class BreakingKeywords(text: String) extends KeywordToken

final case class NonBreakingKeywords(text: String) extends KeywordToken

final case class GroupToken(start: String, close: String, innerTokens: Seq[SyntaxToken]) extends SyntaxToken {
  override def toString = s"$start${innerTokens.mkString(",")}$close"
  def text = toString
}

sealed abstract class GroupingText extends SyntaxToken

final case class OpenGroup(text: String) extends GroupingText

final case class CloseGroup(text: String) extends GroupingText

final case class EscapedText(text: String, quote: Char = '\"') extends SyntaxToken {
  override def toString = s"$quote$text$quote"
}

final case class AnyText(text: String) extends SyntaxToken

case object Comma extends SyntaxToken {
  override val text: String = ","
}

class PrettifierParser extends Parser with Base with Strings {

  def main: Rule1[Seq[SyntaxToken]] = rule("main") { oneOrMoreExteriorToken | noTokens }

  def oneOrMoreExteriorToken: Rule1[Seq[SyntaxToken]] =
    rule("anyTokens") { oneOrMore(exteriorToken, WS) }

  def noTokens: Rule1[Seq[SyntaxToken]] = EMPTY ~ push(Seq.empty)

  def exteriorToken: Rule1[SyntaxToken] =
    rule("anyToken") { anyKeywords | comma | escapedText | anyText | grouping }

  def anyKeywords: Rule1[KeywordToken] = rule("anyKeywords") {
    nonBreakingKeyword | breakingKeywords
  }

  def nonBreakingKeyword: Rule1[NonBreakingKeywords] = rule("nonBreakingKeywords") {
    group (
      keyword("WITH HEADERS") |
        keyword("IS UNIQUE") |
        keyword("ALL") |
        keyword("NULL") |
        keyword("TRUE") |
        keyword("FALSE") |
        keyword("DISTINCT") |
        keyword("END") |
        keyword("NOT") |
        keyword("HAS") |
        keyword("ANY") |
        keyword("NONE") |
        keyword("SINGLE") |
        keyword("OR") |
        keyword("XOR") |
        keyword("AND") |
        keyword("AS") |
        keyword("IN") |
        keyword("IS") |
        keyword("UNIQUE") |
        keyword("BY") |
        keyword("ASSERT") |
        keyword("ASC") |
        keyword("DESC") |
        keyword("SCAN") |
        keyword("FROM") |
        keyword("STARTS WITH") |
        keyword("ENDS WITH") |
        keyword("CONTAINS")
    ) ~> NonBreakingKeywords
  }

  def breakingKeywords: Rule1[BreakingKeywords] = rule("breakingKeywords") {
    joinWithUpdatingBreakingKeywords |
    plainBreakingKeywords |
    updatingBreakingKeywords
  }

  def plainBreakingKeywords: Rule1[BreakingKeywords] = rule("plainBreakingKeywords") {
    group (
      keyword("LOAD CSV") |
      keyword("ORDER BY") |
      keyword("CREATE INDEX ON") |
      keyword("DROP INDEX ON") |
      keyword("CREATE CONSTRAINT ON") |
      keyword("DROP CONSTRAINT ON") |
      keyword("USING PERIODIC COMMIT") |
      keyword("USING INDEX") |
      keyword("USING SCAN") |
      keyword("USING JOIN ON") |
      keyword("OPTIONAL MATCH") |
      keyword("DETACH DELETE") |
      keyword("START") |
      keyword("MATCH") |
      keyword("WHERE") |
      keyword("WITH") |
      keyword("RETURN") |
      keyword("SKIP") |
      keyword("LIMIT") |
      keyword("ORDER BY") |
      keyword("ASC") |
      keyword("DESC") |
      keyword("ON") |
      keyword("WHEN") |
      keyword("CASE") |
      keyword("THEN") |
      keyword("ELSE") |
      keyword("ASSERT") |
      keyword("SCAN") |
      keyword("UNION")
    ) ~> BreakingKeywords
  }

  def joinWithUpdatingBreakingKeywords: Rule1[BreakingKeywords] =
     group( joinedBreakingKeywords ~~ updatingBreakingKeywords ) ~~> ( (k1: BreakingKeywords, k2: BreakingKeywords) =>
       BreakingKeywords(s"${k1.text} ${k2.text}")
     )

  def joinedBreakingKeywords = group( keyword("ON CREATE") | keyword("ON MATCH") ) ~> BreakingKeywords

  def updatingBreakingKeywords: Rule1[BreakingKeywords] = rule("breakingUpdateKeywords") {
    group (
      keyword("CREATE") |
      keyword("SET") |
      keyword("DELETE") |
      keyword("REMOVE") |
      keyword("FOREACH") |
      keyword("MERGE")
    ) ~> BreakingKeywords
  }
  def comma: Rule1[Comma.type] = rule("comma") { "," ~> ( _ => Comma ) }

  def escapedText: Rule1[EscapedText] = rule("string") {
    (((
      ch('\'') ~ StringCharacters('\'') ~ ch('\'') ~ push('\'')
        | ch('"') ~ StringCharacters('"') ~ ch('"')  ~ push('\"')
      ) memoMismatches) suppressSubnodes) ~~> EscapedText
  }

  def anyText: Rule1[AnyText] = rule("anyText") { oneOrMore( (!anyOf(" \n\r\t\f(){}[]")) ~ ANY ) ~> AnyText }

  def grouping: Rule1[GroupToken] = rule("grouping") {
      validGrouping("(", ")") | validGrouping("{", "}") | validGrouping("[", "]")
  }

  def validGrouping(start: String, close: String): Rule1[GroupToken] =
    group( start ~ optional(WS) ~ zeroOrMoreInteriorToken ~ optional(WS) ~ close ) ~~> ( (innerTokens: Seq[SyntaxToken]) => GroupToken(start, close, innerTokens) )

  def zeroOrMoreInteriorToken: Rule1[Seq[SyntaxToken]] = zeroOrMore(interiorToken, WS)

  def interiorToken: Rule1[SyntaxToken] =
    rule("interiorToken") { interiorNonBreakingKeywords | exteriorToken }

  def interiorNonBreakingKeywords = rule("interiorNonBreakingKeywords") {
    keyword("WHERE") ~> NonBreakingKeywords
  }

  def parse(input: String): Seq[SyntaxToken] = parserunners.ReportingParseRunner(main).run(input) match {
    case (output: ParsingResult[_]) if output.matched => output.result.get
    case (output: ParsingResult[Seq[SyntaxToken]])    => throw new SyntaxException(output.parseErrors.mkString("\n"))
  }
}

case object Prettifier extends (String => String) {
  val parser = new PrettifierParser

  def apply(input: String) = {
    val builder = new StringBuilder

    val parsedTokens = parser.parse(input)
    var tokens = flattenTokens(parsedTokens)

    while (tokens.nonEmpty) {
      val tail = tokens.tail
      builder ++= insertBreak(tokens.head, tail)
      tokens = tail
    }

    builder.toString()
  }

  def flattenTokens(tokens: Seq[SyntaxToken]): Seq[SyntaxToken] = {
    val tokenBuilder: mutable.Builder[SyntaxToken, Seq[SyntaxToken]] = Seq.newBuilder[SyntaxToken]
    flattenTokens(tokens, tokenBuilder)
    tokenBuilder.result()
  }

  def flattenTokens(tokens: Seq[SyntaxToken], tokenBuilder: mutable.Builder[SyntaxToken, Seq[SyntaxToken]]) {
    for (token <- tokens) {
      token match {
        case GroupToken(start, close, inner) =>
          tokenBuilder += OpenGroup(start)
          flattenTokens(inner, tokenBuilder)
          tokenBuilder += CloseGroup(close)
        case _ =>
          tokenBuilder += token
      }
    }
  }

  val space = " "
  val newline = System.lineSeparator()

  def insertBreak(token: SyntaxToken, tail: Seq[SyntaxToken]): String = {
    if (tail.isEmpty)
      token.toString
    else {
      (token, tail.head) match {
        // FOREACH : <NEXT>
        case (_: SyntaxToken,         _) if token.text.endsWith("|") => token.toString + space
        case (_: SyntaxToken,         _) if token.text.endsWith(":") => token.toString + space

        // <NON-BREAKING-KW> <NEXT>
        case (_: NonBreakingKeywords, _:SyntaxToken)                 => token.toString + space

        // <HEAD> <BREAKING-KW>
        case (_:SyntaxToken,          _:BreakingKeywords)            => token.toString + newline

        // Never break between keywords
        case (_:KeywordToken,         _:KeywordToken)                => token.toString + space

        // <KW> <OPEN-GROUP>
        case (_:KeywordToken,         _:OpenGroup)                   => token.toString + space

        // <{> <NEXT>
        case (_@OpenGroup("{"),       _:SyntaxToken)                 => token.toString + space

        // <CLOSE-GROUP> <KW>
        case (_:CloseGroup,           _:KeywordToken)                => token.toString + space

        // <GROUPING> <NEXT>
        case (_:GroupingText,        _:SyntaxToken)                 => token.toString

        // <HEAD> <{>
        case (_:SyntaxToken,            OpenGroup("{"))              => token.toString + space

        // <HEAD> <}>
        case (_:SyntaxToken,            CloseGroup("}"))             => token.toString + space

        // <HEAD> <GROUPING>
        case (_:SyntaxToken,          _:GroupingText)               => token.toString

        // <HEAD> <COMMA>
        case (_:SyntaxToken,          Comma)                         => token.toString

        // default
        case _                                                       => token.toString + space
      }
    }
  }
}
