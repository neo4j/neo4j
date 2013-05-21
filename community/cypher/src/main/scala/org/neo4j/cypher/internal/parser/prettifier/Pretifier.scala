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

case class ReservedKeywords(text: String) extends KeywordToken
case class ExtraKeywords(text: String) extends KeywordToken

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

  def parseKeyword: Parser[String] = KEYWORDS

  def parseKeywords: Parser[ReservedKeywords] = rep1(parseKeyword) ^^ {
    case theKeywords => ReservedKeywords(theKeywords.map(_.toLowerCase).mkString(" "))
  }

  def parseExtraKeyword: Parser[String] = EXTRA_KEYWORDS

  def parseExtraKeywords: Parser[ExtraKeywords] = rep1(parseExtraKeyword) ^^ {
    case theKeywords => ExtraKeywords(theKeywords.map(_.toLowerCase).mkString(" "))
  }

  def parseAllKeywords = parseKeywords | parseExtraKeywords

  def parseEscapedText: Parser[EscapedText] = string ^^ EscapedText

  def parseOpenGroup: Parser[OpenGroup] = ( "("|"["|"{" ) ^^ OpenGroup

  def parseCloseGroup: Parser[CloseGroup] = ( ")"|"]"|"}" ) ^^ CloseGroup

  def parseGrouping: Parser[GroupingToken] = parseOpenGroup | parseCloseGroup

  def parseAnyText: Parser[AnyText] = """[^\s(){}\[\]]+""".r ^^ {
    case theAnything => AnyText(theAnything)
  }

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

  val nonBreakingKeywords = Set("on", "scan", "asc", "desc" )

  def insertBreak(token: SyntaxToken, tail: Seq[SyntaxToken]): String = {
    val whitespace = if (tail.isEmpty) "" else " "
    val newline = if (tail.isEmpty) "" else "\n"
    val next = tail.headOption

    (token, next) match {
      // FOREACH : <NEXT>
      case (t: SyntaxToken, _) if t.text.endsWith(":")                     => t + whitespace

      // <NON-BREAKING-EXTRA-KW> <NEXT>
      case (t @ ExtraKeywords(kw), _)                                      => t + whitespace

      // <HEAD> <NON-BREAKING-KW>
      case (t: SyntaxToken, Some(Keywords(kw))) if nonBreakingKeywords(kw) => t + whitespace

      case (t: SyntaxToken, Some(ExtraKeywords(_)))                        => t + whitespace

      // <HEAD> <BREAKING-KW>
      case (t: SyntaxToken, Some(Keywords(_)))                             => t + newline

      // <KW> <OPEN-GROUP>
      case (t: KeywordToken, Some(OpenGroup(_)))                           => t + whitespace

      // <{> <NEXT>
      case (t @ OpenGroup("{"), _)                                         => t.toString + whitespace

      // <GROUPING> <NEXT
      case (t @ GroupingText(_), _)                                        => t.toString

      // <HEAD> <{>
      case (t: SyntaxToken, Some(OpenGroup("{")))                          => t.toString + whitespace

      // <HEAD> <}>
      case (t: SyntaxToken, Some(CloseGroup("}")))                         => t.toString + whitespace

      // <HEAD> <GROUPING>
      case (t: SyntaxToken, Some(GroupingText(_)))                         => t.toString

       // default
      case _                                                               => token + whitespace
    }
  }
}