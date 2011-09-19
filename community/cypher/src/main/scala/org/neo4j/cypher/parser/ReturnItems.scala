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

  def countFunc: Parser[AggregationItem] = ignoreCase("count") ~> parens(returnValues) ^^ { case inner => ValueAggregationItem(Count(inner)) }
  def sumFunc: Parser[AggregationItem] = ignoreCase("sum") ~> parens(returnValues) ^^ { case inner => ValueAggregationItem(Sum(inner)) }
  def minFunc: Parser[AggregationItem] = ignoreCase("min") ~> parens(returnValues) ^^ { case inner => ValueAggregationItem(Min(inner)) }
  def maxFunc: Parser[AggregationItem] = ignoreCase("max") ~> parens(returnValues) ^^ { case inner => ValueAggregationItem(Max(inner)) }
  def avgFunc: Parser[AggregationItem] = ignoreCase("avg") ~> parens(returnValues) ^^ { case inner => ValueAggregationItem(Avg(inner)) }
  def countStar: Parser[AggregationItem] = ignoreCase("count") ~> parens("*") ^^ { case "*" => CountStar()  }

  def aggregate:Parser[AggregationItem] = (countStar | countFunc | sumFunc | minFunc | maxFunc | avgFunc)
}





