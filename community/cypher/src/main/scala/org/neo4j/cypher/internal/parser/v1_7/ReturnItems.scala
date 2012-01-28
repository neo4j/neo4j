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
package org.neo4j.cypher.internal.parser.v1_7

import org.neo4j.cypher.internal.commands._


trait ReturnItems extends Base with Expressions {
  def returnItem: Parser[ReturnItem] = trap(returnExpressions) ^^ {
    case (expression, name) => ExpressionReturnItem(expression, name.replace("`", ""))
  }

  def returnExpressions: Parser[Expression] = nullableProperty | expression | entity

  def aggregateFunctionNames = ignoreCases("count", "sum", "min", "max", "avg", "collect")

  def aggregationFunction: Parser[AggregationItem] = trap(aggregateFunctionNames ~ parens(opt(ignoreCase("distinct")) ~ returnExpressions)) ^^ {
    case (function ~ (distinct ~ inner), unescapedName) => {
      val name = unescapedName.replace("`", "")

      val aggregate = function match {
        case "count" => Count(inner, name)
        case "sum" => Sum(inner, name)
        case "min" => Min(inner, name)
        case "max" => Max(inner, name)
        case "avg" => Avg(inner, name)
        case "collect" => Collect(inner, name)
      }

      if (distinct.isEmpty) {
        ValueAggregationItem(aggregate)
      }
      else {
        ValueAggregationItem(Distinct(aggregate, inner, name))
      }
    }
  }

  def countStar: Parser[AggregationItem] = trap(ignoreCase("count") ~> parens("*")) ^^ {
    case (x, name) => CountStar(name)
  }

  def aggregate: Parser[AggregationItem] = countStar | aggregationFunction
}





