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
  def start: Parser[Start] = ignoreCase("start") ~> rep1sep(nodeByIds | nodeByIndex | nodeByIndexQuery | relsByIds | relsByIndex, ",") ^^ (Start(_: _*))

  def nodeByIds = identity ~ "=" ~ "(" ~ rep1sep(wholeNumber, ",") ~ ")" ^^ {
    case varName ~ "=" ~ "(" ~ id ~ ")" => NodeById(varName, id.map(_.toLong).toSeq: _*)
  }

  def nodeByIndex = identity ~ "=" ~ "(" ~ identity ~ "," ~ identity ~ "," ~ string ~ ")" ^^ {
    case varName ~ "=" ~ "(" ~ index ~ "," ~ key ~ "," ~ value ~ ")" => NodeByIndex(varName, index, key, value)
  }

  def nodeByIndexQuery = identity ~ "=" ~ "(" ~ identity ~ "," ~ string ~ ")" ^^ {
    case varName ~ "=" ~ "(" ~ index ~ "," ~ query ~ ")" => NodeByIndexQuery(varName, index, query)
  }

  def relsByIds = identity ~ "=" ~ "<" ~ rep1sep(wholeNumber, ",") ~ ">" ^^ {
    case varName ~ "=" ~ "<" ~ id ~ ">" => RelationshipById(varName, id.map(_.toLong).toSeq: _*)
  }

  def relsByIndex = identity ~ "=" ~ "<" ~ identity ~ "," ~ identity ~ "," ~ string ~ ">" ^^ {
    case varName ~ "=" ~ "<" ~ index ~ "," ~ key ~ "," ~ value ~ ">" => RelationshipByIndex(varName, index, key, value)
  }
}









