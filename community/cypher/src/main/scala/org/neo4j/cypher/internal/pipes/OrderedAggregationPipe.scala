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
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.symbols.{AnyType, Identifier, SymbolTable}
import org.neo4j.cypher.internal.commands.{Expression, AggregationExpression}
import collection.mutable.Map
import collection.immutable.{Map => ImmutableMap}

// This class can be used to aggregate if the values sub graphs come in the order that they are keyed on
class OrderedAggregationPipe(source: Pipe, val keyExpressions: Seq[Expression], aggregations: Seq[AggregationExpression]) extends PipeWithSource(source) {

  if (keyExpressions.isEmpty)
    throw new ThisShouldNotHappenError("Andres Taylor", "The ordered aggregation pipe should never be used without aggregation keys")

  val symbols: SymbolTable = createSymbols()

  def dependencies: Seq[Identifier] = keyExpressions.flatMap(_.dependencies(AnyType())) ++ aggregations.flatMap(_.dependencies(AnyType()))

  def createSymbols() = {
    val keySymbols = source.symbols.filter(keyExpressions.map(_.identifier.name): _*)
    val aggregateIdentifiers = aggregations.map(_.identifier)

    keySymbols.add(aggregateIdentifiers: _*)
  }

  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = new OrderedAggregator(source.createResults(params), keyExpressions, aggregations)

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "EagerAggregation( keys: [" + keyExpressions.map(_.identifier.name).mkString(", ") + "], aggregates: [" + aggregations.mkString(", ") + "])"
}

private class OrderedAggregator(source: Traversable[Map[String, Any]],
                                returnItems: Seq[Expression],
                                aggregations: Seq[AggregationExpression]) extends Traversable[Map[String, Any]] {
  var currentKey: Option[Seq[Any]] = None
  var aggregationSpool: Seq[AggregationFunction] = null
  val keyColumns = returnItems.map(_.identifier.name)
  val aggregateColumns = aggregations.map(_.identifier.name)

  def getIntermediateResults[U]: Map[String, Any] = {
    (keyColumns.zip(currentKey.get) ++ aggregateColumns.zip(aggregationSpool.map(_.result))).foldLeft(Map[String,Any]())( _ += _ )
  }

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach(m => {
      val key = Some(returnItems.map(_.apply(m)))
      if (currentKey.isEmpty) {
        aggregationSpool = aggregations.map(_.createAggregationFunction)
        currentKey = key
      } else if (key != currentKey) {
        f(getIntermediateResults)

        aggregationSpool = aggregations.map(_.createAggregationFunction)
        currentKey = key
      }

      aggregationSpool.foreach(func => func(m))
    })

    if (currentKey.nonEmpty) {
      f(getIntermediateResults)
    }
  }
}
