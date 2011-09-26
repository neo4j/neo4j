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

import org.neo4j.cypher.commands._
import scala.util.parsing.combinator._

trait StartClause extends JavaTokenParsers with Tokens {
  def start: Parser[Start] = ignoreCase("start") ~> rep1sep(nodeByParam | nodeByIds | nodeByIndex | nodeByIndexQuery | relsByIds | relsByIndex, ",") ^^ (Start(_: _*))

  def param: Parser[Value] = (literalValue | paramValue)
  def paramString: Parser[Value] = (paramValue | literalString)

  def literalString : Parser[Value] = string ^^ { case x => Literal(x) }
  def literalValue : Parser[Value] = identity ^^ { case x => Literal(x) }
  def paramValue: Parser[Value] = "{" ~> identity <~ "}" ^^ { case x => ParameterValue(x) }

  def nodeByParam = identity ~ "=" ~ parens( curly( identity ))^^ {
    case varName ~ "=" ~ paramName => NodeById(varName, ParameterValue(paramName))
  }

  def nodeByIds = identity ~ "=" ~ parens( rep1sep(wholeNumber, ",") ) ^^ {
    case varName ~ "=" ~ id  => NodeById(varName, Literal(id.map(_.toLong)))
  }

  def nodeByIndex = identity ~ "=" ~ "(" ~ identity ~ "," ~ param ~ "," ~ paramString ~ ")" ^^ {
    case varName ~ "=" ~ "(" ~ index ~ "," ~ key ~ "," ~ value ~ ")" => NodeByIndex(varName, index, key, value)
  }

  def nodeByIndexQuery = identity ~ "=" ~ "(" ~ identity ~ "," ~ string ~ ")" ^^ {
    case varName ~ "=" ~ "(" ~ index ~ "," ~ query ~ ")" => NodeByIndexQuery(varName, index, Literal(query))
  }

  def relsByIds = identity ~ "=" ~ "<" ~ rep1sep(wholeNumber, ",") ~ ">" ^^ {
    case varName ~ "=" ~ "<" ~ id ~ ">" => RelationshipById(varName, id.map(_.toLong).toSeq: _*)
  }

  def relsByIndex = identity ~ "=" ~ "<" ~ identity ~ "," ~ identity ~ "," ~ string ~ ">" ^^ {
    case varName ~ "=" ~ "<" ~ index ~ "," ~ key ~ "," ~ value ~ ">" => RelationshipByIndex(varName, index, key, value)
  }
}









