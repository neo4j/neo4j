/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{AggregationExpression, Expression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, MapValue, VirtualValues}

import scala.collection.immutable
import scala.collection.mutable.{Map => MutableMap}

// Eager aggregation means that this pipe will eagerly load the whole resulting sub graphs before starting
// to emit aggregated results.
// Cypher is lazy until it can't - this pipe will eagerly load the full match
case class EagerAggregationPipe(source: Pipe, keyExpressions: Map[String, Expression], aggregations: Map[String, AggregationExpression])
                               (val id: Id = new Id) extends PipeWithSource(source) {

  aggregations.values.foreach(_.registerOwningPipe(this))

  private val expressionOrder: immutable.Seq[(String, Expression)] = keyExpressions.toIndexedSeq

  val groupingFunction: (ExecutionContext, QueryState) => AnyValue = {
    keyExpressions.size match {
      case 1 =>
        val firstExpression = keyExpressions.head._2
        (ctx, state) => firstExpression(ctx)(state)

      case 2 =>
        val e1 = keyExpressions.head._2
        val e2 = keyExpressions.last._2
        (ctx, state) => VirtualValues.list(e1(ctx)(state), e2(ctx)(state))

      case 3 =>
        val e1 = keyExpressions.head._2
        val e2 = keyExpressions.tail.head._2
        val e3 = keyExpressions.last._2
        (ctx, state) => VirtualValues.list(e1(ctx)(state), e2(ctx)(state), e3(ctx)(state))

      case _ =>
        val expressions = keyExpressions.values.toSeq
        (ctx, state) => VirtualValues.list(expressions.map(e => e(ctx)(state)): _*)
    }
  }

  def createResultFunction(newMap: MutableMap[String, AnyValue], groupingKey: AnyValue): Unit =
    keyExpressions.size match {
      case 1 =>
        val tuple = keyExpressions.head._1 -> groupingKey
        newMap += tuple
      case 2 =>
        val t2 = groupingKey.asInstanceOf[ListValue]
        newMap += keyExpressions.head._1 -> t2.head +=
          keyExpressions.last._1 -> t2.last
      case 3 =>
        val t3 = groupingKey.asInstanceOf[ListValue]
        newMap += keyExpressions.head._1 -> t3.value(0) +=
          keyExpressions.tail.head._1  -> t3.value(1) +=
          keyExpressions.last._1  -> t3.value(2)
      case _ =>
        val listOfValues = groupingKey.asInstanceOf[ListValue]
        for (i <- 0 until keyExpressions.size) {
          val (k, v) = expressionOrder(i)
          val value: AnyValue = listOfValues.value(i)
          newMap += (k -> value)
        }
    }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    implicit val s = state

    val result = MutableMap[AnyValue, Seq[AggregationFunction]]()
    val keyNames = keyExpressions.keySet.toList
    val aggregationNames: IndexedSeq[String] = aggregations.keys.toIndexedSeq
    val keyNamesSize = keyNames.size
    val mapSize = keyNamesSize + aggregationNames.size

    def createEmptyResult(params: MapValue): Iterator[ExecutionContext] = {
      val newMap = MutableMaps.empty
      val aggregationNamesAndFunctions = aggregationNames zip aggregations.map(_._2.createAggregationFunction.result)

      aggregationNamesAndFunctions.toMap
        .foreach { case (name, zeroValue) => newMap += name -> zeroValue}
      Iterator.single(ExecutionContext(newMap))
    }

    // This code is not pretty. It's full of asInstanceOf calls and other things that might irk you.
    // You'll just have to trust that the original authors spent time profiling and making sure that this
    // code runs really fast.
    // If you feel like cleaning it up - please make sure to not regress in performance. This is a hot spot.
    def createResults(groupingKey: AnyValue, aggregator: scala.Seq[AggregationFunction]): ExecutionContext = {
      val newMap = MutableMaps.create(mapSize)
      createResultFunction(newMap, groupingKey)
      (aggregationNames zip aggregator.map(_.result)).foreach(newMap += _)
      ExecutionContext(newMap)
    }

    input.foreach(ctx => {
      val groupingValue: AnyValue = groupingFunction(ctx, state)
      val functions = result.getOrElseUpdate(groupingValue, {
        val aggregateFunctions: Seq[AggregationFunction] = aggregations.map(_._2.createAggregationFunction).toIndexedSeq
        aggregateFunctions
      })
      functions.foreach(func => func(ctx)(state))
    })

    if (result.isEmpty && keyNames.isEmpty) {
      createEmptyResult(state.params)
    } else {
      result.map {
        case (key, aggregator) => createResults(key, aggregator)
      }.toIterator
    }
  }
}
