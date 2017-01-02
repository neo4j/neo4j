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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.compiler.v3_0.commands.predicates.Equivalent
import org.neo4j.cypher.internal.compiler.v3_0.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable

import scala.collection.GenTraversableOnce
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}

// Eager aggregation means that this pipe will eagerly load the whole resulting sub graphs before starting
// to emit aggregated results.
// Cypher is lazy until it can't - this pipe will eagerly load the full match
case class EagerAggregationPipe(source: Pipe, keyExpressions: Set[String], aggregations: Map[String, AggregationExpression])
                               (val estimatedCardinality: Option[Double] = None)
                               (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with RonjaPipe with NoEffectsPipe {

  val symbols: SymbolTable = createSymbols()

  private def createSymbols() = {
    val keyVariables = keyExpressions.map(id => id -> source.symbols.evaluateType(id, CTAny)).toMap
    val aggrVariables = aggregations.map {
      case (innerId, exp) => innerId -> exp.getType(source.symbols)
    }

    SymbolTable(keyVariables ++ aggrVariables)
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    // This is the temporary storage used while the aggregation is going on
    val result = MutableMap[Equals, Seq[AggregationFunction]]()
    val keyNames = keyExpressions.toList
    val aggregationNames: Seq[String] = aggregations.keys.toSeq
    val keyNamesSize = keyNames.size
    val mapSize = keyNamesSize + aggregationNames.size

    def createEmptyResult(params: Map[String, Any]): Iterator[ExecutionContext] = {
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
    def createResults(key: Any, aggregator: scala.Seq[AggregationFunction]): ExecutionContext = {
      val newMap = MutableMaps.create(mapSize)

      //add key values
      keyNamesSize match {
        case 1 =>
          newMap += keyNames.head -> key.asInstanceOf[Equivalent].originalValue
        case 2 =>
          val t2 = key.asInstanceOf[(Equivalent, Equivalent)]
          newMap += keyNames.head -> t2._1.originalValue +=
                    keyNames.last -> t2._2.originalValue
        case 3 =>
          val t3 = key.asInstanceOf[(Equivalent, Equivalent, Equivalent)]
          newMap += keyNames.head -> t3._1.originalValue +=
                    keyNames.tail.head -> t3._2.originalValue +=
                    keyNames.last -> t3._3.originalValue
        case _ =>
          val listOfValues = key.asInstanceOf[List[Equivalent]]
          (keyNames zip listOfValues.map(_.originalValue)).foreach(newMap += _)
      }

      //add aggregated values
      (aggregationNames zip aggregator.map(_.result)).foreach(newMap += _)

      ExecutionContext(newMap)
    }

    input.foreach(ctx => {
      val groupValues: Equals = keyNamesSize match {
        case 1 => Equivalent(ctx(keyNames.head))
        case 2 => (Equivalent(ctx(keyNames.head)),Equivalent(ctx(keyNames.last)))
        case 3 => (Equivalent(ctx(keyNames.head)),Equivalent(ctx(keyNames.tail.head)),Equivalent(ctx(keyNames.last)))
        case _ => keyNames.map( k => Equivalent(ctx(k)))
      }
      val functions = result.getOrElseUpdate(groupValues, {
        val aggregateFunctions: Seq[AggregationFunction] = aggregations.map(_._2.createAggregationFunction).toSeq
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

  def planDescriptionWithoutCardinality = source.planDescription.
                        andThen(this.id, "EagerAggregation", variables, Arguments.KeyNames(keyExpressions.toSeq))

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
