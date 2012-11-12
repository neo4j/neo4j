/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v1_6

import scala.util.parsing.combinator._
import org.neo4j.cypher.internal.commands.{Literal, ParameterExpression, Expression}
import org.neo4j.helpers.ThisShouldNotHappenError

abstract class Base extends JavaTokenParsers {
  val keywords = List("start", "where", "return", "limit", "skip", "order", "by")

  def ignoreCase(str: String): Parser[String] = ("""(?i)\b""" + str + """\b""").r ^^ (x => x.toLowerCase)

  def onlyOne[T](msg: String, inner: Parser[List[T]]): Parser[T] = Parser {
    in => inner.apply(in) match {
      case x: NoSuccess => x
      case Success(result, pos) => if (result.size > 1)
        Failure("INNER" + msg, pos)
      else
        Success(result.head, pos)
    }
  }

  def ignoreCases(strings: String*): Parser[String] = ignoreCases(strings.toList)

  def ignoreCases(strings: List[String]): Parser[String] = strings match {
    case List(x) => ignoreCase(x)
    case first :: rest => ignoreCase(first) | ignoreCases(rest)
    case _ => throw new ThisShouldNotHappenError("Andres", "Something went wrong if we get here.")
  }

  def comaList[T](inner: Parser[T]): Parser[List[T]] =
    rep1sep(inner, ",") |
      rep1sep(inner, ",") ~> opt(",") ~> failure("trailing coma")

  def identity: Parser[String] = (nonKeywordIdentifier | escapedIdentity)

  def nonKeywordIdentifier: Parser[String] =
    not(ignoreCases(keywords: _*)) ~> ident |
      ignoreCases(keywords: _*) ~> failure("reserved keyword")

  def lowerCaseIdent = ident ^^ (c => c.toLowerCase)

  def number: Parser[String] = """-?(\d+(\.\d*)?|\d*\.\d+)""".r

  def optParens[U](q: => Parser[U]): Parser[U] = parens(q) | q

  def parens[U](inner: => Parser[U]) =
    ("(" ~> inner <~ ")"
      | "(" ~> inner ~> failure("Unclosed parenthesis"))

  def curly[U](inner: => Parser[U]) =
    ("{" ~> inner <~ "}"
      | "{" ~> inner ~> failure("Unclosed curly brackets"))

  def escapedIdentity: Parser[String] = ("`(``|[^`])*`").r ^^ (str => stripQuotes(str).replace("``", "`"))

  def stripQuotes(s: String) = s.substring(1, s.length - 1)

  def positiveNumber: Parser[String] = """\d+""".r
  def anything: Parser[String] = """[.\s]""".r

  def string: Parser[String] = (stringLiteral | apostropheString) ^^ (str => stripQuotes(str))

  def apostropheString: Parser[String] = ("\'" + """([^'\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""" + "\'").r

  def regularLiteral = ("/" + """([^"\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*?""" + "/").r ^^ (x => Literal(stripQuotes(x)))

  def parameter: Parser[Expression] = curly(identity | wholeNumber) ^^ (x => ParameterExpression(x))

  override def failure(msg: String): Parser[Nothing] = "" ~> super.failure("INNER" + msg)
}