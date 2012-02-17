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


trait ReturnClause extends Base with Expressions {
  def column = expressionColumn

  def returnItem: Parser[ReturnItem] = trap(expression) ^^ {
    case (expression, name) => ReturnItem(expression, name.replace("`", ""))
  }

  def returns =
    (returnsClause
      | ignoreCase("return") ~> failure("return column list expected")
      | failure("expected return clause"))


  def alias: Parser[Option[String]] = opt(ignoreCase("as") ~> identity)

  def expressionColumn: Parser[ReturnItem] = returnItem ~ alias ^^ {
    case col ~ Some(newName) => col.rename(newName)
    case col ~ None => col
  }

  def returnsClause: Parser[(Return, Option[Aggregation])] = ignoreCase("return") ~> opt(ignoreCase("distinct")) ~ comaList(column) ^^ {
    case distinct ~ items => {
      val (aggregationItems, returnItems) = items.partition(_.expression.containsAggregate)
      val columnName = items.map(_.columnName).toList

      val none: Option[Aggregation] = distinct match {
        case Some(x) => Some(Aggregation())
        case None => None
      }

      val aggregation = aggregationItems match {
        case List() => none
        case _ => Some(Aggregation(aggregationItems.map(_.asInstanceOf[ReturnItem]): _*))
      }


      (Return(columnName, returnItems: _*), aggregation)
    }
  }
}
