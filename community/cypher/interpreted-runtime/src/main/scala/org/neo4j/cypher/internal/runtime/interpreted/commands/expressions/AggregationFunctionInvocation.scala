/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer
import org.neo4j.internal.kernel.api.procs.UserAggregationUpdater
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstanceWithObjectReferences
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

abstract class AggregationFunctionInvocation(arguments: IndexedSeq[Expression])
    extends AggregationExpression {

  override def createAggregationFunction(memoryTracker: MemoryTracker): AggregationFunction = {
    memoryTracker.allocateHeap(AggregationFunctionInvocation.SHALLOW_SIZE)
    new AggregationFunction {
      private var innerReducer: UserAggregationReducer = _
      private var innerUpdater: UserAggregationUpdater = _

      override def result(state: QueryState): AnyValue = {
        assertLoaded(state)
        innerUpdater.applyUpdates()
        innerReducer.result()
      }

      override def apply(data: ReadableRow, state: QueryState): Unit = {
        val length = arguments.length
        val argValues = new Array[AnyValue](length)
        var i = 0
        while (i < length) {
          argValues(i) = arguments(i).apply(data, state)
          i += 1
        }
        updater(state).update(argValues)
      }

      private def updater(state: QueryState) = {
        assertLoaded(state)
        innerUpdater
      }

      private def assertLoaded(state: QueryState): Unit = {
        if (innerReducer == null) {
          innerReducer = call(state)
          innerUpdater = innerReducer.newUpdater()
        }
      }
    }
  }

  override def children: Seq[AstNode[_]] = arguments

  protected def call(state: QueryState): UserAggregationReducer
}

object AggregationFunctionInvocation {

  def apply(
    signature: UserFunctionSignature,
    arguments: IndexedSeq[Expression]
  ): AggregationFunctionInvocation = {
    if (signature.builtIn) BuiltInAggregationFunctionInvocation(signature.id, arguments)
    else UserAggregationFunctionInvocation(signature.id, arguments)
  }

  // AggregationFunction instances are usually created from nested anonymous classes with some outer context:
  // This should give a low-end approximate.
  final val SHALLOW_SIZE: Long =
    shallowSizeOfInstanceWithObjectReferences(2) + // AggregationFunction: 1 ref + 1 $outer
      shallowSizeOfInstanceWithObjectReferences(1) + // UserDefinedAggregator: 1 ref
      shallowSizeOfInstanceWithObjectReferences(
        3
      ) // UserAggregator: 2 refs + 1 this$0 (one is a new SecurityContext, but not counted)
}

case class UserAggregationFunctionInvocation(fcnId: Int, arguments: IndexedSeq[Expression])
    extends AggregationFunctionInvocation(arguments) {

  override def rewrite(f: Expression => Expression): Expression =
    f(UserAggregationFunctionInvocation(fcnId, arguments.map(a => a.rewrite(f))))

  override protected def call(state: QueryState): UserAggregationReducer = {
    val query = state.query
    query.aggregateFunction(fcnId, query.procedureCallContext(fcnId))
  }
}

case class BuiltInAggregationFunctionInvocation(fcnId: Int, arguments: IndexedSeq[Expression])
    extends AggregationFunctionInvocation(arguments) {

  override def rewrite(f: Expression => Expression): Expression =
    f(BuiltInAggregationFunctionInvocation(fcnId, arguments.map(a => a.rewrite(f))))

  override protected def call(state: QueryState): UserAggregationReducer = {
    val query = state.query
    query.builtInAggregateFunction(fcnId, query.procedureCallContext(fcnId))
  }
}
