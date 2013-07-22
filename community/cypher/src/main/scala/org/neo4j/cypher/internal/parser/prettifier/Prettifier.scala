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
package org.neo4j.cypher.internal.parser.prettifier

import org.neo4j.cypher.internal.parser.v2_0.Base
import org.neo4j.cypher.SyntaxException

trait SyntaxToken {
  val text: String

  override def toString = text
}

trait KeywordToken extends SyntaxToken {
  override def toString = text.toUpperCase
}

case class BreakingKeywords(text: String) extends KeywordToken

case class NonBreakingKeywords(text: String) extends KeywordToken

case class AnyText(text: String) extends SyntaxToken

case class EscapedText(text: String) extends SyntaxToken {
  override def toString = s"'${text}'"
}

trait GroupingToken extends SyntaxToken

case class OpenGroup(text: String) extends GroupingToken

case class CloseGroup(text: String) extends GroupingToken

class PrettifierParser extends Base {

  def parseReservedKeyword: Parser[String] = KEYWORDS

  def nonBreakingKeywords: Parser[String] =
    ALL | NULL | TRUE | FALSE | DISTINCT | END | NOT | HAS | ANY | NONE | SINGLE | OR | XOR | AND | AS | INDEX | IN |
      IS | UNIQUE | BY | ASSERT | ASC | DESC | SCAN | ON

  def rep1Keywords[T](p: Parser[String])(f: String => T): Parser[T] = rep1(p) ^^ {
    case theMatch => f(theMatch.map(_.toLowerCase).mkString(" "))
  }

  def keywordSeq[T](parsers: Parser[String]*)(f: String => T): Parser[T] =
    parsers.reduce((a, b) => a ~ b ^^ {
      case fst ~ snd => fst + " " + snd
    }) ^^ (_.toLowerCase) ^^ f

  def parseAllKeywords =
  // first rule wins
      keywordSeq(ON, CREATE)(BreakingKeywords) |
      keywordSeq(ON, MATCH)(BreakingKeywords) |
      keywordSeq(ORDER, BY)(BreakingKeywords) |
      rep1Keywords(nonBreakingKeywords)(NonBreakingKeywords) |
      rep1Keywords(parseReservedKeyword)(BreakingKeywords)

  def parseEscapedText: Parser[EscapedText] = string ^^ EscapedText

  def parseOpenGroup: Parser[OpenGroup] = ("(" | "[" | "{") ^^ OpenGroup

  def parseCloseGroup: Parser[CloseGroup] = (")" | "]" | "}") ^^ CloseGroup

  def parseGrouping: Parser[GroupingToken] = parseOpenGroup | parseCloseGroup

  def parseAnyText: Parser[AnyText] = """[^\s(){}\[\]]+""".r ^^ AnyText

  def parseToken: Parser[SyntaxToken] = parseAllKeywords | parseGrouping | parseEscapedText | parseAnyText

  def query: Parser[Seq[SyntaxToken]] = rep(parseToken)

  def parse(input: String): Seq[SyntaxToken] = {
    parseAll(query, input) match {
      case Success(tokens, _) => tokens
      case NoSuccess(msg, _)  => throw new SyntaxException(msg)
    }
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
