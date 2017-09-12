/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{AggregationExpression, Expression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, MapValue, VirtualValues}

import scala.collection.immutable
import scala.collection.mutable.{Map => MutableMap}

// Eager aggregation means that this pipe will eagerly load the whole resulting sub graphs before starting
// to emit aggregated results.
// Cypher is lazy until it can't - this pipe will eagerly load the full match
case class EagerAggregationSlottedPipe(source: Pipe,
                                       pipelineInformation: PipelineInformation,
                                       groupingExpressions: Map[Int, Expression],
                                       aggregations: Map[Int, AggregationExpression])(val id: Id = new Id)
  extends PipeWithSource(source) {

  aggregations.values.foreach(_.registerOwningPipe(this))
  groupingExpressions.values.foreach(_.registerOwningPipe(this))

  private val (aggregationOffsets: IndexedSeq[Int], aggregationFunctions: IndexedSeq[AggregationExpression]) = {
    val (a,b) = aggregations.unzip
    (a.toIndexedSeq, b.toIndexedSeq)
  }

  private val expressionOrder: immutable.Seq[(Int, Expression)] = groupingExpressions.toIndexedSeq

  val groupingFunction: (ExecutionContext, QueryState) => AnyValue = {
    groupingExpressions.size match {
      case 1 =>
        val firstExpression = groupingExpressions.head._2
        (ctx, state) => firstExpression(ctx, state)

      case 2 =>
        val e1 = groupingExpressions.head._2
        val e2 = groupingExpressions.last._2
        (ctx, state) => VirtualValues.list(e1(ctx, state), e2(ctx, state))

      case 3 =>
        val e1 = groupingExpressions.head._2
        val e2 = groupingExpressions.tail.head._2
        val e3 = groupingExpressions.last._2
        (ctx, state) => VirtualValues.list(e1(ctx, state), e2(ctx, state), e3(ctx, state))

      case _ =>
        val expressions = groupingExpressions.values.toSeq
        (ctx, state) => VirtualValues.list(expressions.map(e => e(ctx, state)): _*)
    }
  }

  def addGroupingValuesToResult(context: PrimitiveExecutionContext, groupingKey: AnyValue): Unit =
    groupingExpressions.size match {
      case 1 =>
        context.setRefAt(groupingExpressions.head._1, groupingKey)
      case 2 =>
        val t2 = groupingKey.asInstanceOf[ListValue]
        context.setRefAt(groupingExpressions.head._1, t2.head())
        context.setRefAt(groupingExpressions.tail.head._1, t2.last())
      case 3 =>
        val t3 = groupingKey.asInstanceOf[ListValue]
        context.setRefAt(groupingExpressions.head._1, t3.value(0))
        context.setRefAt(groupingExpressions.tail.head._1, t3.value(1))
        context.setRefAt(groupingExpressions.last._1, t3.value(2))
      case _ =>
        val listOfValues = groupingKey.asInstanceOf[ListValue]
        for (i <- 0 until groupingExpressions.size) {
          val (k, v) = expressionOrder(i)
          val value: AnyValue = listOfValues.value(i)
          context.setRefAt(k, value)
        }
    }

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {

    val result = MutableMap[AnyValue, Seq[AggregationFunction]]()

    // Used when we have no input and no grouping expressions. In this case, we'll return a single row
    def createEmptyResult(params: MapValue): Iterator[ExecutionContext] = {
      val context = PrimitiveExecutionContext(pipelineInformation)
      val aggregationOffsetsAndFunctions = aggregationOffsets zip aggregations
        .map(_._2.createAggregationFunction.result(state))

      aggregationOffsetsAndFunctions.toMap.foreach {
        case (offset, zeroValue) => context.setRefAt(offset, zeroValue)
      }
      Iterator.single(context)
    }

    def writeAggregationResultToContext(groupingKey: AnyValue, aggregator: Seq[AggregationFunction]): ExecutionContext = {
      val context = PrimitiveExecutionContext(pipelineInformation)
      addGroupingValuesToResult(context, groupingKey)
      (aggregationOffsets zip aggregator.map(_.result(state))).foreach {
        case (offset, value) => context.setRefAt(offset, value)
      }
      context
    }

    // Consume all input and aggregate
    input.foreach(ctx => {
      val groupingValue: AnyValue = groupingFunction(ctx, state)
      val functions = result.getOrElseUpdate(groupingValue, aggregationFunctions.map(_.createAggregationFunction))
      functions.foreach(func => func(ctx, state))
    })

    // Write the produced aggregation map to the output pipeline
    if (result.isEmpty && groupingExpressions.isEmpty) {
      createEmptyResult(state.params)
    } else {
      result.map {
        case (key, aggregator) => writeAggregationResultToContext(key, aggregator)
      }.toIterator
    }
  }
}
