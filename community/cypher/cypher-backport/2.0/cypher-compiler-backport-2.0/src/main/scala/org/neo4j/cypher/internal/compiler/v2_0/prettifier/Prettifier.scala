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

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_0.parser.{Base, Strings}
import org.parboiled.scala._

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

final case class EscapedText(text: String, quote: Char = '\"') extends SyntaxToken {
  override def toString = s"$quote$text$quote"
}

final case class AnyText(text: String) extends SyntaxToken

case object Comma extends SyntaxToken {
  override val text: String = ","
}


class PrettifierParser extends Parser with Base with Strings {

  def reservedKeyword: Rule0 = rule("reservedKeyword") {
    keyword("START") | keyword("CREATE") | keyword("SET") | keyword("DELETE") | keyword("FOREACH") |
    keyword("MATCH") | keyword("WHERE") | keyword("WITH") | keyword("RETURN") | keyword("SKIP") |
    keyword("LIMIT") | keyword("ORDER") | keyword("BY") | keyword("ASC") | keyword("DESC") |
    keyword("ON") | keyword("WHEN") | keyword("CASE") | keyword("THEN") | keyword("ELSE") |
    keyword("DROP") | keyword("USING") | keyword("MERGE") | keyword("CONSTRAINT") | keyword("ASSERT") |
    keyword("SCAN") | keyword("REMOVE") | keyword("UNION") | keyword("LOAD")
  }

  def nonBreakingKeyword: Rule0 = rule("nonBreakingKeyword") {
    keyword("ALL") | keyword("NULL") | keyword("TRUE") | keyword("FALSE") | keyword("DISTINCT") |
    keyword("END") | keyword("NOT") | keyword("HAS") | keyword("ANY") | keyword("NONE") | keyword("SINGLE") |
    keyword("OR") | keyword("XOR") | keyword("AND") | keyword("AS") | keyword("INDEX") | keyword("IN") |
    keyword("IS") | keyword("UNIQUE") | keyword("BY") | keyword("ASSERT") | keyword("ASC") | keyword("DESC") |
    keyword("SCAN") | keyword("ON") | keyword("FROM") | keyword("HEADERS")| keyword("CSV")
  }

  def allKeywords: Rule1[KeywordToken] = rule("allKeywords") {
    ( group( keyword("LOAD") ~~ keyword("CSV") ) ~> BreakingKeywords ) |
    ( group( keyword("WITH") ~~ keyword("HEADERS") ) ~> NonBreakingKeywords ) |
    ( group( keyword("ON") ~~ keyword("CREATE") ) ~> BreakingKeywords ) |
    ( group( keyword("ON") ~~ keyword("MATCH") ) ~> BreakingKeywords ) |
    ( group( keyword("ORDER") ~~ keyword("BY") ) ~> BreakingKeywords ) |
    ( group( keyword("OPTIONAL") ~~ keyword("MATCH") ) ~> BreakingKeywords ) |
    ( group( oneOrMore(nonBreakingKeyword, WS) ) ~> NonBreakingKeywords ) |
    ( group( oneOrMore(reservedKeyword, WS) ) ~> BreakingKeywords )
  }

  // Keywords for which we do not insert breaks while inside a grouping
  def unbrokenKeywords: Rule1[KeywordToken] =
    rule("allUnbrokenKeywords") { keyword("WHERE") ~> NonBreakingKeywords }

  def openGroup: Rule1[OpenGroup] = rule("openGroup") { ( "(" | "[" | "{" ) ~> OpenGroup }

  def closeGroup: Rule1[CloseGroup] = rule("closeGroup") { ( ")" | "]" | "}" ) ~> CloseGroup }

  def grouped(inner: Rule1[Seq[SyntaxToken]]) = rule("grouped") {
    (openGroup ~~ zeroOrMore(inner, WS) ~~ closeGroup) ~~>
      ((start: OpenGroup, inner: List[Seq[SyntaxToken]], end: CloseGroup) => {
        val builder = Seq.newBuilder[SyntaxToken]
        builder.sizeHint(inner, 2)
        builder += start
        inner.foreach( builder ++= _ )
        builder += end
        builder.result()
      })
  }

    def escapedText : Rule1[EscapedText] = rule("string") {
      (((
        ch('\'') ~ StringCharacters('\'') ~ ch('\'') ~ push('\'')
          | ch('"') ~ StringCharacters('"') ~ ch('"')  ~ push('\"')
        ) memoMismatches) suppressSubnodes) ~~> (EscapedText(_, _))
    }

  def comma: Rule1[Comma.type] = rule("comma") { "," ~> ( _ => Comma ) }

  def anyText: Rule1[AnyText] = rule("anyText") { oneOrMore( (!anyOf(" \n\r\t\f(){}[]")) ~ ANY ) ~> AnyText }

  def noTokens: Rule1[Seq[SyntaxToken]] = EMPTY ~ push(Seq.empty)

  def simpleTokens: Rule1[Seq[SyntaxToken]] =
    rule("simpleTokens") { seq1( allKeywords | comma | escapedText | anyText ) }

  def anyTokens: Rule1[Seq[SyntaxToken]] =
    rule("anyTokens") { flat( oneOrMore( simpleTokens | grouped( anyUnbrokenTokens ), WS ) ) }

  def simpleUnbrokenTokens: Rule1[Seq[SyntaxToken]] =
    rule("simpleUnbrokenTokens") { seq1( unbrokenKeywords ) | simpleTokens }

  def anyUnbrokenTokens: Rule1[Seq[SyntaxToken]] =
    rule("anyUnbrokenTokens") { flat( oneOrMore( simpleUnbrokenTokens | grouped( anyUnbrokenTokens ), WS ) ) }

  def main: Rule1[Seq[SyntaxToken]] = rule("main") { anyTokens | noTokens }

  def parse(input: String): Seq[SyntaxToken] = parserunners.ReportingParseRunner(main).run(input) match {
    case (output: ParsingResult[_]) if output.matched => output.result.get
    case (output: ParsingResult[Seq[SyntaxToken]])    => throw new SyntaxException(output.parseErrors.mkString("\n"))
  }

  private def seq1[T](r: Rule1[T]): Rule1[Seq[T]] = r ~~> ((t: T) => Seq(t))

  private def flat[T](r: Rule1[Seq[Seq[T]]]): Rule1[Seq[T]] = r ~~> ((s: Seq[Seq[T]]) => s.flatten )
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

        // <HEAD> <COMMA>
        case (_:SyntaxToken,          Comma)                         => token.toString

        // default
        case _                                                       => token.toString + space
      }
    }
  }
}
