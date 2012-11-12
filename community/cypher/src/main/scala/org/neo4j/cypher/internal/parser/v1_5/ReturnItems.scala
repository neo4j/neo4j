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
package org.neo4j.cypher.internal.parser.v1_5


import scala.util.parsing.combinator._
import org.neo4j.cypher.internal.commands._

trait ReturnItems extends JavaTokenParsers with Tokens with Values {
  def returnItem: Parser[ReturnItem] = returnValues ^^ {
    case value => ReturnItem(value, value.identifier.name)
  }

  def returnValues: Parser[Expression] = nullableProperty | value | entityValue

  def functionNames = ignoreCase("count") | ignoreCase("sum") | ignoreCase("min") | ignoreCase("max") | ignoreCase("avg") | ignoreCase("collect")

  def aggregationFunction: Parser[ReturnItem] = functionNames ~ parens(opt(ignoreCase("distinct")) ~ returnValues) ^^ {
    case name ~ (distinct ~ inner) => {
      val (aggregate,columnName) = name match {
        case "count" => (Count(inner), "count(" + inner.identifier.name + ")")
        case "sum" => (Sum(inner), "sum(" + inner.identifier.name + ")")
        case "min" => (Min(inner), "min(" + inner.identifier.name + ")")
        case "max" => (Max(inner), "max(" + inner.identifier.name + ")")
        case "avg" => (Avg(inner), "avg(" + inner.identifier.name + ")")
        case "collect" => (Collect(inner), "collect(" + inner.identifier.name + ")")
      }

      if (distinct.isEmpty) {
        ReturnItem(aggregate, columnName)
      }
      else {
        val innerName = aggregate.identifier.name
        val name = innerName.substring(0, innerName.indexOf("(")) + "(distinct " + inner.identifier.name + ")"

        ReturnItem(Distinct(aggregate, inner), name)
      }
    }
  }

  def countStar: Parser[ReturnItem] = ignoreCase("count") ~> parens("*") ^^ {
    case "*" => ReturnItem(CountStar(), "count(*)")
  }

  def aggregate: Parser[ReturnItem] = (countStar | aggregationFunction)
}





