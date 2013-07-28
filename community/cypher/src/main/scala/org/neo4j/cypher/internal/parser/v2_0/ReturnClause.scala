/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.cypher.internal.commands._
import expressions.{Identifier, AggregationExpression}
import org.neo4j.cypher.SyntaxException


trait ReturnClause extends Base with Expressions {
  def column : Parser[ReturnColumn] = returnItem ~ alias ^^ {
    case col ~ Some(newName) => col.rename(newName)
    case col ~ None => col
  } | "*" ^^^ AllIdentifiers()

  def returnItem: Parser[ReturnItem] = trap(exprOrPred) ^^ {
    case (expression, name) => ReturnItem(expression, name)
  }

  def returns =
    (returnsClause
      | RETURN ~> failure("return column list expected")
      | failure("expected return clause"))

  def returnsClause: Parser[(Return, Option[Seq[AggregationExpression]])] = RETURN ~> columnList

  def alias: Parser[Option[String]] = opt(AS ~> identity)

  def columnList:Parser[(Return, Option[Seq[AggregationExpression]])]  = opt(DISTINCT) ~ commaList(column) ^^ {
    case distinct ~ returnItems => {
      val columnName = returnItems.map(_.name).toList

      val none = distinct match {
        case Some(x) => Some(Seq())
        case None => None
      }

      val aggregationExpressions = returnItems.filter(_.isInstanceOf[ReturnItem]).map(_.asInstanceOf[ReturnItem]).
        flatMap(_.expression.filter(_.isInstanceOf[AggregationExpression])).
        map(_.asInstanceOf[AggregationExpression])

      val aggregation = aggregationExpressions match {
        case List() => none
        case _ => Some(aggregationExpressions)
      }

      (Return(columnName, returnItems: _*), aggregation)
    }
  }

  def withSyntax = WITH ~> columnList

  def withClause: Parser[(Return, Option[Seq[AggregationExpression]])] = withSyntax ^^ (columns => {

    val problemColumns = columns._1.returnItems.flatMap {
      case ReturnItem(_, _, true) => None
      case ReturnItem(Identifier(_), _, _) => None
      case ri => Some(ri.name)
    }
    if (problemColumns.nonEmpty) {
      throw new SyntaxException("These columns can't be listen in the WITH statement without renaming: " + problemColumns.mkString(","))
    }

    columns
  })
}
