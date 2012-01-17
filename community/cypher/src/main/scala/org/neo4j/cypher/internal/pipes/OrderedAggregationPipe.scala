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
import org.neo4j.cypher.commands.{AggregationItem, ReturnItem}
import java.lang.String
import org.neo4j.cypher.symbols.{Identifier, SymbolTable}
import org.neo4j.helpers.ThisShouldNotHappenError

// This class can be used to aggregate if the values sub graphs come in the order that they are keyed on
class OrderedAggregationPipe(source: Pipe, val returnItems: Seq[ReturnItem], aggregations: Seq[AggregationItem]) extends PipeWithSource(source) {

  if (returnItems.isEmpty)
    throw new ThisShouldNotHappenError("Andres Taylor", "The ordered aggregation pipe should never be used without aggregation keys")

  val symbols: SymbolTable = createSymbols()

  def dependencies: Seq[Identifier] = returnItems.flatMap(_.dependencies) ++ aggregations.flatMap(_.dependencies)

  def createSymbols() = {
    val keySymbols = source.symbols.filter(returnItems.map(_.columnName): _*)
    val aggregatedColumns = aggregations.map(_.concreteReturnItem.identifier)

    keySymbols.add(aggregatedColumns: _*)
  }

  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = new OrderedAggregator(source.createResults(params), returnItems, aggregations)

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "EagerAggregation( keys: [" + returnItems.map(_.columnName).mkString(", ") + "], aggregates: [" + aggregations.mkString(", ") + "])"
}

private class OrderedAggregator(source: Traversable[Map[String, Any]],
                                returnItems: Seq[ReturnItem],
                                aggregations: Seq[AggregationItem]) extends Traversable[Map[String, Any]] {
  var currentKey: Option[Seq[Any]] = None
  var aggregationSpool: Seq[AggregationFunction] = null
  val keyColumns = returnItems.map(_.columnName)
  val aggregateColumns = aggregations.map(_.columnName)

  def getIntermediateResults[U]() = (keyColumns.zip(currentKey.get) ++ aggregateColumns.zip(aggregationSpool.map(_.result))).toMap

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach(m => {
      val key = Some(returnItems.map(_.apply(m)))
      if (currentKey.isEmpty) {
        aggregationSpool = aggregations.map(_.createAggregationFunction)
        currentKey = key
      } else if (key != currentKey) {
        f(getIntermediateResults())

        aggregationSpool = aggregations.map(_.createAggregationFunction)
        currentKey = key
      }

      aggregationSpool.foreach(func => func(m))
    })

    if (currentKey.nonEmpty) {
      f(getIntermediateResults())
    }
  }
}
