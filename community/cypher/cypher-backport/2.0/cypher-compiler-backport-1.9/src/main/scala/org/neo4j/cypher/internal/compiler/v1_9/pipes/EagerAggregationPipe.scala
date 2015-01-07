/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes

import aggregation.AggregationFunction
import org.neo4j.cypher.internal.compiler.v1_9.symbols._
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.{Expression, AggregationExpression}
import collection.mutable.{Map => MutableMap}
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.data.SimpleVal

// Eager aggregation means that this pipe will eagerly load the whole resulting sub graphs before starting
// to emit aggregated results.
// Cypher is lazy until it can't - this pipe will eagerly load the full match
class EagerAggregationPipe(source: Pipe, val keyExpressions: Map[String, Expression], aggregations: Map[String, AggregationExpression])
  extends PipeWithSource(source) {
  def oldKeyExpressions: Seq[Expression] = keyExpressions.values.toSeq

  val symbols: SymbolTable = createSymbols()

  private def createSymbols() = {
    val typeExtractor: ((String, Expression)) => (String, CypherType) = {
      case (id, exp) => id -> exp.getType(source.symbols)
    }

    val keyIdentifiers = keyExpressions.map(typeExtractor)
    val aggrIdentifiers = aggregations.map(typeExtractor)

    SymbolTable(keyIdentifiers ++ aggrIdentifiers)
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    // This is the temporary storage used while the aggregation is going on
    val result = MutableMap[NiceHasher, (ExecutionContext, Seq[AggregationFunction])]()
    val keyNames: Seq[String] = keyExpressions.map(_._1).toSeq
    val aggregationNames: Seq[String] = aggregations.map(_._1).toSeq

    def createResults(key: NiceHasher, aggregator: scala.Seq[AggregationFunction], ctx: ExecutionContext): ExecutionContext = {
      val newMap = MutableMaps.empty

      //add key values
      (keyNames zip key.original).foreach(newMap += _)

      //add aggregated values
      (aggregationNames zip aggregator.map(_.result)).foreach(newMap += _)

      ctx.newFrom(newMap)
    }

    def createEmptyResult(params:Map[String,Any]): Iterator[ExecutionContext] = {
      val newMap = MutableMaps.empty
      val aggregationNamesAndFunctions = aggregationNames zip aggregations.map(_._2.createAggregationFunction.result)

      aggregationNamesAndFunctions.toMap
        .foreach { case (name, zeroValue) => newMap += name -> zeroValue  }
      Iterator.single(ExecutionContext(newMap))
    }

    input.foreach(ctx => {
      val groupValues: NiceHasher = new NiceHasher(keyNames.map(ctx))
      val aggregateFunctions: Seq[AggregationFunction] = aggregations.map(_._2.createAggregationFunction).toSeq
      val (_, functions) = result.getOrElseUpdate(groupValues, (ctx, aggregateFunctions))
      functions.foreach(func => func(ctx)(state))
    })

    if (result.isEmpty && keyNames.isEmpty) {
      createEmptyResult(state.params)
    } else {
      result.map {
        case (key, (ctx, aggregator)) => createResults(key, aggregator, ctx)
      }.toIterator
    }
  }

  override def executionPlanDescription =
    source.executionPlanDescription
      .andThen(this, "EagerAggregation",
        "keys" -> SimpleVal.fromIterable(oldKeyExpressions),
        "aggregates" -> SimpleVal.fromIterable(aggregations))

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    keyExpressions.foreach(_._2.throwIfSymbolsMissing(symbols))
    aggregations.foreach(_._2.throwIfSymbolsMissing(symbols))
  }

  override def isLazy = false
}
