/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util.function.Supplier

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.LongArrayHashMap
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.opencypher.v9_0.util.attribution.Id

import scala.collection.JavaConverters._

//  This is a pipe can be used when the grouping is on all primitive long columns.
case class EagerAggregationSlottedPrimitivePipe(source: Pipe,
                                                slots: SlotConfiguration,
                                                readGrouping: Array[Int], // Offsets into the long array of the current execution context
                                                writeGrouping: Array[Int], // Offsets into the long array of the current execution context
                                                aggregations: Map[Int, AggregationExpression])
                                               (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  aggregations.values.foreach(_.registerOwningPipe(this))

  private val (aggregationOffsets: IndexedSeq[Int], aggregationFunctions: IndexedSeq[AggregationExpression]) = {
    val (a, b) = aggregations.unzip
    (a.toIndexedSeq, b.toIndexedSeq)
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {

    val result = new LongArrayHashMap[Seq[AggregationFunction]](32, readGrouping.length)
    val keys = new Array[Long](readGrouping.length)

    def createResultRow(groupingKey: Array[Long], aggregator: Seq[AggregationFunction]): ExecutionContext = {
      val context = SlottedExecutionContext(slots)
      setKeyToCtx(context, groupingKey)
      (aggregationOffsets zip aggregator.map(_.result(state))).foreach {
        case (offset, value) => context.setRefAt(offset, value)
      }
      context
    }

    def setKeyFromCtx(ctx: ExecutionContext): Unit = {
      var i = 0
      while (i < readGrouping.length) {
        keys(i) = ctx.getLongAt(readGrouping(i))
        i += 1
      }
    }

    def setKeyToCtx(ctx: ExecutionContext, key: Array[Long]): Unit = {
      var i = 0
      while (i < writeGrouping.length) {
        ctx.setLongAt(writeGrouping(i), key(i))
        i += 1
      }
    }

    val supplier: Supplier[Seq[AggregationFunction]] = new Supplier[Seq[AggregationFunction]] {
      override def get(): Seq[AggregationFunction] = aggregationFunctions.map(_.createAggregationFunction)
    }

    // Consume all input and aggregate
    input.foreach(ctx => {
      setKeyFromCtx(ctx)
      val aggregationFunctions = result.computeIfAbsent(keys, supplier)
      aggregationFunctions.foreach(func => func(ctx, state))
    })

    // Write the produced aggregation map to the output pipeline
    result.iterator().asScala.map {
      e: java.util.Map.Entry[Array[Long], Seq[AggregationFunction]] => createResultRow(e.getKey, e.getValue)
    }
  }
}
