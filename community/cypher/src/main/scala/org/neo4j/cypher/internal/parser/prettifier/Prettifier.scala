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

import org.neo4j.cypher.internal.parser.v2_0.{StringLiteral, Base}
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

object Keywords {
  def unapply(k: KeywordToken): Option[String] = Some(k.text)
}

case class AnyText(text: String) extends SyntaxToken

case class EscapedText(text: String) extends SyntaxToken {
  override def toString = s"'${text}'"
}

trait GroupingToken extends SyntaxToken

case class OpenGroup(text: String) extends GroupingToken
case class CloseGroup(text: String) extends GroupingToken

object GroupingText {
  def unapply(g: GroupingToken): Option[String] = Some(g.text)
}

class PrettifierParser extends Base {

  def parseReservedKeyword: Parser[String] = KEYWORDS

  def parseExtraKeyword: Parser[String] =
    ALL | NULL | TRUE | FALSE | DISTINCT | END | NOT | HAS | ANY | NONE |
    SINGLE | OR | XOR | AND | AS | INDEX | IN  | IS | ASSERT | UNIQUE  | BY

  def rep1Keywords[T](p: Parser[String])(f: String => T): Parser[T] = rep1(p) ^^ {
    case theMatch => f(theMatch.map(_.toLowerCase).mkString(" "))
  }

  def keywordSeq[T](parsers: Parser[String]*)(f: String => T): Parser[T] =
    parsers.reduce( (a, b) => a ~ b ^^ { case fst ~ snd => fst + " " + snd } ) ^^ (_.toLowerCase) ^^ f

  def parseAllKeywords =
    // first rule wins
    keywordSeq(ASC)(NonBreakingKeywords) |
    keywordSeq(DESC)(NonBreakingKeywords) |
    keywordSeq(SCAN)(NonBreakingKeywords) |
    keywordSeq(ON, CREATE)(BreakingKeywords) |
    keywordSeq(ON, MATCH)(BreakingKeywords) |
    keywordSeq(ON)(NonBreakingKeywords) |
    keywordSeq(ORDER, BY)(BreakingKeywords) |
    rep1Keywords(parseReservedKeyword)(BreakingKeywords) |
    rep1Keywords(parseExtraKeyword)(NonBreakingKeywords)

  def parseEscapedText: Parser[EscapedText] = string ^^ EscapedText

  def parseOpenGroup: Parser[OpenGroup] = ( "("|"["|"{" ) ^^ OpenGroup

  def parseCloseGroup: Parser[CloseGroup] = ( ")"|"]"|"}" ) ^^ CloseGroup

  def parseGrouping: Parser[GroupingToken] = parseOpenGroup | parseCloseGroup

  def parseAnyText: Parser[AnyText] = """[^\s(){}\[\]]+""".r ^^ AnyText

  def parseToken: Parser[SyntaxToken] = parseAllKeywords | parseGrouping | parseEscapedText | parseAnyText

  def query: Parser[Seq[SyntaxToken]] = rep( parseToken )

  def parse(input: String): Seq[SyntaxToken] = {
    parseAll(query, input) match {
      case Success(tokens, _) => tokens
      case NoSuccess(msg, _)  => throw new SyntaxException(msg)
    }
  }
}

case object Prettifier extends (String => String)
{
  val parser = new PrettifierParser

  def apply(input: String) = {
    val builder = new StringBuilder

    var tokens = parser.parse(input)
    while ( tokens.nonEmpty ) {
      val tail = tokens.tail
      builder ++= insertBreak(tokens.head, tail)
      tokens = tail
    }

    builder.toString()
  }

  val whitespace = " "
  val newline = "\n"

  def insertBreak(token: SyntaxToken, tail: Seq[SyntaxToken]): String = {
    if (tail.isEmpty)
      token.toString
    else
    {
      (token, tail.head) match {
        // FOREACH : <NEXT>
        case (t: SyntaxToken, _) if t.text.endsWith(":") => t.toString + whitespace

        // <NON-BREAKING-KW> <NEXT>
        case (t @ NonBreakingKeywords(kw), _)            => t.toString + whitespace

        // Never break between keywords
        case (t @ Keywords(_), Keywords(_))              => t.toString + whitespace

        // <HEAD> <BREAKING-KW>
        case (t: SyntaxToken, BreakingKeywords(_))       => t.toString + newline

        // <KW> <OPEN-GROUP>
        case (t: KeywordToken, OpenGroup(_))             => t.toString + whitespace

        // <{> <NEXT>
        case (t @ OpenGroup("{"), _)                     => t.toString + whitespace

        // <CLOSE-GROUP> <KW>
        case (t @ CloseGroup(_), Keywords(_))            => t.toString + whitespace

        // <GROUPING> <NEXT>
        case (t @ GroupingText(_), _)                    => t.toString

        // <HEAD> <{>
        case (t: SyntaxToken, OpenGroup("{"))            => t.toString + whitespace

        // <HEAD> <}>
        case (t: SyntaxToken, CloseGroup("}"))           => t.toString + whitespace

        // <HEAD> <GROUPING>
        case (t: SyntaxToken, GroupingText(_))           => t.toString

         // default
        case (t, _)                                      => t.toString + whitespace
      }
    }
  }
}