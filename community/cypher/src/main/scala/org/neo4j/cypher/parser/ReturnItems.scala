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

trait ReturnItems extends JavaTokenParsers with Tokens with Values {
  def returnItem: Parser[ReturnItem] = returnValues ^^ {
    case value => ValueReturnItem(value)
  }

  def returnValues: Parser[Value] = nullableProperty | value | entityValue

  private def lowerCaseIdent = ident ^^ {
    case c => c.toLowerCase
  }

  def aggregationValueFunction: Parser[AggregationItem] = lowerCaseIdent ~ "(" ~ ( returnValues )~ ")" ^^ {
    case "count" ~ "(" ~ inner ~ ")" => ValueAggregationItem(Count(inner))
    case "sum" ~ "(" ~ inner ~ ")" => ValueAggregationItem(Sum(inner))
    case "min" ~ "(" ~ inner ~ ")" => ValueAggregationItem(Min(inner))
    case "max" ~ "(" ~ inner ~ ")" => ValueAggregationItem(Max(inner))
    case "avg" ~ "(" ~ inner ~ ")" => ValueAggregationItem(Avg(inner))
  }

  def countStar: Parser[AggregationItem] = ignoreCase("count") ~> "(*)" ^^ {
    case "(*)" => CountStar()
  }

  def aggregate:Parser[AggregationItem] = countStar  | aggregationValueFunction
}





