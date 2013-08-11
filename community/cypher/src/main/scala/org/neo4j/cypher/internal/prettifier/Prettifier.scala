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
package org.neo4j.cypher.internal.prettifier

import org.parboiled.scala._
import org.neo4j.cypher.internal.parser.v2_0.rules.Base
import org.neo4j.cypher.internal.parser.v2_0.rules.LiteralSupport
import org.neo4j.cypher.SyntaxException

sealed abstract class SyntaxToken {
  def text: String

  override def toString = text
}

sealed abstract class KeywordToken extends SyntaxToken {
  override def toString = text.toUpperCase
}

final case class BreakingKeywords(text: String) extends KeywordToken
final case class NonBreakingKeywords(text: String) extends KeywordToken

sealed abstract class GroupingToken extends SyntaxToken
final case class OpenGroup(text: String) extends GroupingToken
final case class CloseGroup(text: String) extends GroupingToken

final case class EscapedText(text: String) extends SyntaxToken {
  override def toString = s"'${text}'"
}

final case class AnyText(text: String) extends SyntaxToken

class PrettifierParser extends Parser with Base with LiteralSupport {

  def reservedKeyword: Rule0 = rule("reservedKeyword") {
    keyword("START") | keyword("CREATE") | keyword("SET") | keyword("DELETE") | keyword("FOREACH") |
    keyword("MATCH") | keyword("WHERE") | keyword("WITH") | keyword("RETURN") | keyword("SKIP") |
    keyword("LIMIT") | keyword("ORDER") | keyword("BY") | keyword("ASC") | keyword("DESC") |
    keyword("ON") | keyword("WHEN") | keyword("CASE") | keyword("THEN") | keyword("ELSE") |
    keyword("DROP") | keyword("USING") | keyword("MERGE") | keyword("CONSTRAINT") | keyword("ASSERT") |
    keyword("SCAN") | keyword("REMOVE") | keyword("UNION")
  }

  def nonBreakingKeyword: Rule0 = rule("nonBreakingKeyword") {
    keyword("ALL") | keyword("NULL") | keyword("TRUE") | keyword("FALSE") | keyword("DISTINCT") |
    keyword("END") | keyword("NOT") | keyword("HAS") | keyword("ANY") | keyword("NONE") | keyword("SINGLE") |
    keyword("OR") | keyword("XOR") | keyword("AND") | keyword("AS") | keyword("INDEX") | keyword("IN") |
    keyword("IS") | keyword("UNIQUE") | keyword("BY") | keyword("ASSERT") | keyword("ASC") | keyword("DESC") |
    keyword("SCAN") | keyword("ON")
  }

  def allKeywords: Rule1[KeywordToken] = rule("allKeywords") {
    ( group( keyword("ON") ~~ keyword("CREATE") ) ~> BreakingKeywords ) |
    ( group( keyword("ON") ~~ keyword("MATCH") ) ~> BreakingKeywords ) |
    ( group( keyword("ORDER") ~~ keyword("BY") ) ~> BreakingKeywords ) |
    ( group( oneOrMore(nonBreakingKeyword, WS) ) ~> NonBreakingKeywords ) |
    ( group( oneOrMore(reservedKeyword, WS) ) ~> BreakingKeywords )
  }

  def grouping: Rule1[GroupingToken] = rule("grouping") { openGroup | closeGroup }
  def openGroup: Rule1[OpenGroup] = rule("openGroup") { ( "(" | "[" | "{" ) ~> OpenGroup }
  def closeGroup: Rule1[CloseGroup] = rule("closeGroup") { ( ")" | "]" | "}" ) ~> CloseGroup }

  def escapedText : Rule1[EscapedText] = rule("string") {
    (((
      ch('\'') ~ StringCharacters('\'') ~ ch('\'')
        | ch('"') ~ StringCharacters('"') ~ ch('"')
      ) memoMismatches) suppressSubnodes) ~~> EscapedText
  }

  def anyText: Rule1[AnyText] = rule("anyText") { oneOrMore( (!anyOf(" \n\r\t\f(){}[]")) ~ ANY ) ~> AnyText }

  def anyToken: Rule1[SyntaxToken] = rule("anyToken") { allKeywords | grouping | escapedText | anyText }

  def main: Rule1[Seq[SyntaxToken]] = rule("main") { zeroOrMore(anyToken, WS) }

  def parse(input: String): Seq[SyntaxToken] = parserunners.ReportingParseRunner(main).run(input) match {
    case (output: ParsingResult[_]) if output.matched => output.result.get
    case (output: ParsingResult[Seq[SyntaxToken]])    => throw new SyntaxException(output.parseErrors.mkString("\n"))
  }
}

case object Prettifier extends (String => String) {
  val parser = new PrettifierParser

  def apply(input: String) = {
    val builder = new StringBuilder

    var tokens = parser.parse(input)
    while (tokens.nonEmpty) {
      val tail = tokens.tail
      builder ++= insertBreak(tokens.head, tail)
      tokens = tail
    }

    builder.toString()
  }

  val space = " "
  val newline = System.getProperty("line.separator")

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

        // Never break between keywords
        case (_:KeywordToken,         _:KeywordToken)                => token.toString + space

        // <HEAD> <BREAKING-KW>
        case (_:SyntaxToken,          _:BreakingKeywords)            => token.toString + newline

        // <KW> <OPEN-GROUP>
        case (_:KeywordToken,         _:OpenGroup)                   => token.toString + space

        // <{> <NEXT>
        case (_@OpenGroup("{"),       _:SyntaxToken)                 => token.toString + space

        // <CLOSE-GROUP> <KW>
        case (_:CloseGroup,           _:KeywordToken)                => token.toString + space

        // <GROUPING> <NEXT>
        case (_:GroupingToken,        _:SyntaxToken)                 => token.toString

        // <HEAD> <{>
        case (_:SyntaxToken,            OpenGroup("{"))              => token.toString + space

        // <HEAD> <}>
        case (_:SyntaxToken,            CloseGroup("}"))             => token.toString + space

        // <HEAD> <GROUPING>
        case (_:SyntaxToken,          _:GroupingToken)               => token.toString

        // default
        case _                                                       => token.toString + space
      }
    }
  }
}
