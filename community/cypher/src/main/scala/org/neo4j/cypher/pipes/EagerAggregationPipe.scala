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
import collection.Seq
import org.neo4j.cypher.commands.{AggregationItem, ReturnItem}
import org.neo4j.cypher.{SyntaxException, SymbolTable}
import java.lang.String

// Eager aggregation means that this pipe will eagerly load the whole resulting subgraphs before starting
// to emit aggregated results.
// Cypher is lazy until it has to - this pipe makes stops the lazyness
class EagerAggregationPipe(source: Pipe, returnItems: Seq[ReturnItem], aggregations: Seq[AggregationItem]) extends Pipe {
  val symbols: SymbolTable = createSymbols()

  def createSymbols() = {
    val keyColumns = returnItems.map(x => source.symbols.getOrElse(x.columnName, ()=>throw new SyntaxException("This should not happen - did not find column `" + x.columnName + "`")))
    val aggregatedColumns = aggregations.map(_.concreteReturnItem.identifier)
    new SymbolTable(keyColumns).add(aggregatedColumns)
  }

  aggregations.foreach(_.assertDependencies(source))

  def foreach[U](f: Map[String, Any] => U) {
    // This is the temporary storage used while the aggregation is going on
    val result = collection.mutable.Map[Seq[Any], Seq[AggregationFunction]]()
    val keyNames = returnItems.map(_.columnName)
    val aggregationNames = aggregations.map(_.identifier.name)

    source.foreach(m => {
      val groupValues = keyNames.map(m(_))
      val functions = result.getOrElseUpdate(groupValues, aggregations.map(_.createAggregationFunction))
      functions.foreach(func => func(m))
    })

    result.foreach {
      case (key, value: Seq[AggregationFunction]) => {
        val elems = keyNames.zip(key) ++ aggregationNames.zip(value.map(_.result))
        f(elems.toMap)
      }
    }
  }

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "EagerAggregation( keys: [" + returnItems.map(_.columnName).mkString(", ") + "], aggregates: [" + aggregations.mkString(", ") + "])"
}