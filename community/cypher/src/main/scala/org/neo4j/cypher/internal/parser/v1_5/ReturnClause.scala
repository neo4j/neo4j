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

trait ReturnClause extends JavaTokenParsers with Tokens with ReturnItems {


  def returns: Parser[(Return, Option[Aggregation])] = ignoreCase("return") ~> opt(ignoreCase("distinct")) ~ rep1sep((aggregate | returnItem), ",") ^^ {
    case distinct ~ items => {
      val (aggregationItems, returnItems) = items.partition(_.expression.exists(_.isInstanceOf[AggregationExpression]))

      /*
      An DISTINCT is created by using a normal aggregation with no aggregate functions.
      The key columns is a set, and so a DISTINCT is produced.
       */
      val aggregation = (aggregationItems, distinct) match {
        case (List(), Some(_)) => Some(Aggregation())
        case (List(), None) => None
        case _ => Some(Aggregation(aggregationItems: _*))
      }

      (Return(items.map(_.columnName).toList, returnItems: _*), aggregation)
    }
  }


}





