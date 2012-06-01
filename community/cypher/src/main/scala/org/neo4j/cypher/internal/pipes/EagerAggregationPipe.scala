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
import org.neo4j.cypher.internal.symbols.{AnyType, Identifier, SymbolTable}
import org.neo4j.cypher.internal.commands.{Expression, AggregationExpression}
import collection.mutable.{Map => MutableMap}

// Eager aggregation means that this pipe will eagerly load the whole resulting sub graphs before starting
// to emit aggregated results.
// Cypher is lazy until it has to - this pipe makes stops the lazyness
class EagerAggregationPipe(source: Pipe, val keyExpressions: Seq[Expression], aggregations: Seq[AggregationExpression]) extends PipeWithSource(source) {
  val symbols: SymbolTable = createSymbols()

  def dependencies: Seq[Identifier] = keyExpressions.flatMap(_.dependencies(AnyType())) ++ aggregations.flatMap(_.dependencies(AnyType()))

  def createSymbols() = {
    val map = keyExpressions.map(_.identifier.name)
    val keySymbols = source.symbols.filter(map: _*)
    val aggregatedColumns = aggregations.map(_.identifier)

    keySymbols.add(aggregatedColumns: _*)
  }

  def createResults(state: QueryState): Traversable[ExecutionContext] = {
    // This is the temporary storage used while the aggregation is going on
    val result = MutableMap[NiceHasher, (ExecutionContext,Seq[AggregationFunction])]()
    val keyNames = keyExpressions.map(_.identifier.name)
    val aggregationNames = aggregations.map(_.identifier.name)

    source.createResults(state).foreach(ctx => {
      val groupValues: NiceHasher = new NiceHasher(keyNames.map(ctx(_)))
      val (_,functions) = result.getOrElseUpdate(groupValues, (ctx, aggregations.map(_.createAggregationFunction)))
      functions.foreach(func => func(ctx))
    })

    if (result.isEmpty && keyNames.isEmpty) {
      createEmptyResult(aggregationNames)
    } else result.map {
      case (key, (ctx,aggregator)) =>
        val newMap = MutableMaps.create

        //add key values
        keyNames.zip(key.original).foreach( newMap += _)

        //add aggregated values
        aggregationNames.zip(aggregator.map(_.result)).foreach( newMap += _ )

        ctx.newFrom(newMap)
    }
  }


  private def createEmptyResult(aggregationNames: Seq[String]): Traversable[ExecutionContext] = {
    val newMap = MutableMaps.create
    val aggregationNamesAndFunctions = aggregationNames zip aggregations.map(_.createAggregationFunction.result)
    aggregationNamesAndFunctions.toMap
      .foreach {
      case (name, zeroValue) => newMap += name -> zeroValue
    }
    Traversable(ExecutionContext(newMap))
  }

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "EagerAggregation( keys: [" + keyExpressions.map(_.identifier.name).mkString(", ") + "], aggregates: [" + aggregations.mkString(", ") + "])"
}