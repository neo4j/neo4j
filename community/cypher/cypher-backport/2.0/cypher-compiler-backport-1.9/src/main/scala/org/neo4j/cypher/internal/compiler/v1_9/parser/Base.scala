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
package org.neo4j.cypher.internal.compiler.v1_9.parser

import scala.util.parsing.combinator._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.{ParameterExpression, Expression, Literal}

abstract class Base extends JavaTokenParsers {
  val keywords = List("start", "create", "set", "delete", "foreach", "match", "where",
    "with", "return", "skip", "limit", "order", "by", "asc", "ascending", "desc", "descending")

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

  def reduce[A,B](in:Seq[(Seq[A], Seq[B])]):(Seq[A], Seq[B]) = if (in.isEmpty) (Seq(),Seq()) else in.reduce((a, b) => (a._1 ++ b._1, a._2 ++ b._2))

  def ignoreCases(strings: String*): Parser[String] = ignoreCases(strings.toList)

  def ignoreCases(strings: List[String]): Parser[String] = strings match {
    case List(x) => ignoreCase(x)
    case first :: rest => ignoreCase(first) | ignoreCases(rest)
    case _ => throw new ThisShouldNotHappenError("Andres", "Something went wrong if we get here.")
  }

  def commaList[T](inner: Parser[T]): Parser[List[T]] =
    rep1sep(inner, ",") |
      rep1sep(inner, ",") ~> opt(",") ~> failure("trailing coma")

  def identity: Parser[String] = nonKeywordIdentifier | escapedIdentity

  def trap[T](inner: Parser[T]): Parser[(T, String)] = Parser {
    in => {
      inner.apply(in) match {
        case Success(result,input) => Success((result, input.source.subSequence(in.offset, input.offset).toString.trim), input  )
        case Failure(msg,input) => Failure(msg,input)
        case Error(msg,input) => Error(msg,input)
      }
    }
  }

  def nonKeywordIdentifier: Parser[String] =
    not(ignoreCases(keywords: _*)) ~> ident |
      ignoreCases(keywords: _*) ~> failure("reserved keyword")

  def lowerCaseIdent = ident ^^ (c => c.toLowerCase)

  def number: Parser[String] = """-?(\d+(\.\d*)?|\d*\.\d+)""".r

  def optParens[U](q: => Parser[U]): Parser[U] = q | parens(q)

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

  def failure(msg:String, input:Input) = Failure("INNER" + msg, input)

  /**
   * A parser that returns either an auto-generated named pointing to the position in the string,
   * or the name, if it can be found
   * @return
   */
  def optionalName : Parser[String] = Parser {
    in => (generatedName ~ opt(identity)).apply(in) match {
      case Success(_ ~ Some(name), rest) => Success(name, rest)
      case Success(name ~ None, rest)    => Success(name, rest)
      case _                             => Failure("expected an optional name", in)
    }
  }

  /**
   * This parser returns an identifier pointing to this point in the string.
   * Note: it doesn't eat anything from the input string
   * @return
   */
  def generatedName : Parser[String] = Parser {
    in => Success("  UNNAMED" + in.offset, in)
  }

}
