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
package org.neo4j.cypher.pipes.aggregation

import org.neo4j.cypher.commands.ReturnItem
import org.neo4j.cypher.SyntaxException

class AvgFunction(returnItem: ReturnItem) extends AggregationFunction with Plus {
  private var count: Int = 0
  private var sofar: Any = 0

  def result: Any = divide(sofar, count)

  def apply(data: Map[String, Any]) {
    val value = returnItem(data)(returnItem.columnName)

    value match {
      case null =>
      case number: Number => {
        count = count + 1
        sofar = plus(sofar, number)
      }
      case _ => throw new SyntaxException("AVG can only handle values of Number type, or null.")
    }
  }
}