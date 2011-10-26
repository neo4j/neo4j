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
import org.neo4j.cypher.commands.{ParameterValue, Literal, Value}

trait SkipLimitClause extends JavaTokenParsers with Tokens {
  def skip: Parser[Value] = ignoreCase("skip") ~> numberOrParam ^^ (x => x)

  def limit: Parser[Value] = ignoreCase("limit") ~> numberOrParam ^^ (x => x)

  private def numberOrParam: Parser[Value] = (parameter|positiveNumber) ^^ {
    case x:ParameterValue => x
    case x:String => Literal(x.toInt)
  }
}





