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
package org.neo4j.cypher.internal.pipes

import aggregation.AggregationFunction
import collection.Seq
import java.lang.String
import org.neo4j.cypher.internal.commands.{AggregationExpression, ReturnItem}
import org.neo4j.cypher.internal.symbols.{AnyType, Identifier, SymbolTable}

// Eager aggregation means that this pipe will eagerly load the whole resulting subgraphs before starting
// to emit aggregated results.
// Cypher is lazy until it has to - this pipe makes stops the lazyness
class EagerAggregationPipe(source: Pipe, val returnItems: Seq[ReturnItem], aggregations: Seq[AggregationExpression]) extends PipeWithSource(source) {
  val symbols: SymbolTable = createSymbols()

  def dependencies: Seq[Identifier] = returnItems.flatMap(_.dependencies) ++ aggregations.flatMap(_.dependencies(AnyType()))

  def createSymbols() = {
    val keySymbols = source.symbols.filter(returnItems.map(_.expressionName): _*)
    val aggregatedColumns = aggregations.map(_.identifier)

    keySymbols.add(aggregatedColumns: _*)
  }

  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = {
    // This is the temporary storage used while the aggregation is going on
    val result = collection.mutable.Map[NiceHasher, Seq[AggregationFunction]]()
    val keyNames = returnItems.filterNot(_.expression.containsAggregate).map(_.expressionName)
    val aggregationNames = aggregations.map(_.identifier.name)

    source.createResults(params).foreach(m => {
      val groupValues: NiceHasher = new NiceHasher(keyNames.map(m(_)))
      val functions = result.getOrElseUpdate(groupValues, aggregations.map(_.createAggregationFunction))
      functions.foreach(func => func(m))
    })

    result.map {
      case (key, value: Seq[AggregationFunction]) => {
        val elems = keyNames.zip(key.original) ++ aggregationNames.zip(value.map(_.result))
        elems.toMap
      }
    }
  }

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "EagerAggregation( keys: [" + returnItems.map(_.columnName).mkString(", ") + "], aggregates: [" + aggregations.mkString(", ") + "])"
}