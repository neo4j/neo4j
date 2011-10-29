package org.neo4j.cypher.parser

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import scala.util.parsing.combinator._
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.commands.{Literal, ParameterValue, Value}

trait Tokens extends JavaTokenParsers {
  val keywords = List("start", "where", "return", "limit", "skip", "order", "by")

  def ignoreCase(str: String): Parser[ String ] = ( """(?i)\Q""" + str + """\E""" ).r ^^ ( x => x.toLowerCase )

  def identity: Parser[ String ] = ( nonKeywordIdentifier | escapedIdentity )

  def nonKeywordIdentifier: Parser[ String ] = ident ^^ {
    case str => {
      if( keywords.contains(str.toLowerCase) ) {
        throw new SyntaxException(str + " is a reserved keyword and may not be used here.")
      } else {
        str
      }
    }
  }

  def lowerCaseIdent = ident ^^ {
    case c => c.toLowerCase
  }

  def optParens[ U ](q: => Parser[ U ]): Parser[ U ] = parens(q) | q

  def parens[ U ](inner: => Parser[ U ]) = "(" ~> inner <~ ")"

  def curly[ U ](inner: => Parser[ U ]) = "{" ~> inner <~ "}"

  def escapedIdentity: Parser[ String ] = ( "`(``|[^`])*`" ).r ^^ ( str => stripQuotes(str).replace("``", "`") )

  def stripQuotes(s: String) = s.substring(1, s.length - 1)

  def positiveNumber: Parser[ String ] = """\d+""".r

  def string: Parser[ String ] = ( stringLiteral | apostropheString ) ^^ ( str => stripQuotes(str) )

  def apostropheString: Parser[ String ] = ( "\'" + """([^'\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""" + "\'" ).r

  def regularLiteral = ( "/" + """([^"\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""" + "/" ).r ^^ ( x => Literal(stripQuotes(x)) )

  def parameter: Parser[ Value ] = curly(identity | wholeNumber) ^^ ( x => ParameterValue(x) )
}
















