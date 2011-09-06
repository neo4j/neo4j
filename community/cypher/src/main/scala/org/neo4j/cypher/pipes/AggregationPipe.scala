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
package org.neo4j.cypher.pipes

import aggregation.AggregationFunction
import org.neo4j.cypher.SymbolTable
import org.neo4j.cypher.commands.{AggregationItem, ReturnItem}

class AggregationPipe(source: Pipe, returnItems: Seq[ReturnItem], aggregations: Seq[AggregationItem]) extends Pipe {
  val symbols: SymbolTable = source.symbols.add(aggregations.map(_.identifier))

  aggregations.foreach(_.assertDependencies(source))

  def foreach[U](f: Map[String, Any] => U) {
    val result = collection.mutable.Map[Seq[Any], Seq[AggregationFunction]]()
    val valueNames = returnItems.map(_.columnName)
    val aggregationNames = aggregations.map(_.identifier.name)

    source.foreach(m => {
      val groupValues = valueNames.map(m(_))
      val functions = result.getOrElseUpdate(groupValues, aggregations.map(_.createAggregationFunction))
      functions.foreach(func => func(m))
    })

    result.foreach {
      case (key, value: Seq[AggregationFunction]) => {
        val elems = valueNames.zip(key) ++ aggregationNames.zip(value.map(_.result))
        f(elems.toMap)
      }
    }
  }
}