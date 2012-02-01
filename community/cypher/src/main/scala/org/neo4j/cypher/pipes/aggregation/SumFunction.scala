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
package org.neo4j.cypher.pipes.aggregation

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.commands.Value

class SumFunction(value:Value) extends AggregationFunction with Plus {
  private var soFar: Any = null

  def result: Any = soFar match {
    case null => 0
    case _ => soFar
  }

  def apply(data: Map[String, Any]) {
    val number = value(data)

    if(number != null && !number.isInstanceOf[Number]) {
      throw new SyntaxException("Sum can only handle values of Number type, or null.")
    }

    soFar = plus(soFar, number)
  }
}