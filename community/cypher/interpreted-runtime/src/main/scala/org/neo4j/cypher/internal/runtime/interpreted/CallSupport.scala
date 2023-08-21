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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.collection.RawIterator
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer
import org.neo4j.values.AnyValue

/**
 * This class contains helpers for calling procedures, user-defined functions and user-defined aggregations.
 */
object CallSupport {

  type KernelProcedureCall = Array[AnyValue] => RawIterator[Array[AnyValue], ProcedureException]

  def callFunction(procedures: Procedures, id: Int, args: Array[AnyValue], context: ProcedureCallContext): AnyValue = {
    procedures.functionCall(id, args, context)
  }

  def callBuiltInFunction(
    procedures: Procedures,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): AnyValue = {
    procedures.builtInFunctionCall(id, args, context)
  }

  def callReadOnlyProcedure(
    procedures: Procedures,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    callProcedure(args, procedures.procedureCallRead(id, _, context))

  def callReadWriteProcedure(
    procedures: Procedures,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    callProcedure(args, procedures.procedureCallWrite(id, _, context))

  def callSchemaWriteProcedure(
    procedures: Procedures,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    callProcedure(args, procedures.procedureCallSchema(id, _, context))

  def callDbmsProcedure(
    procedures: Procedures,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    callProcedure(args, procedures.procedureCallDbms(id, _, context))

  def aggregateFunction(procedures: Procedures, id: Int, context: ProcedureCallContext): UserAggregationReducer = {
    procedures.aggregationFunction(id, context)
  }

  def builtInAggregateFunction(
    procedures: Procedures,
    id: Int,
    context: ProcedureCallContext
  ): UserAggregationReducer = {
    procedures.builtInAggregationFunction(id, context)
  }

  private def callProcedure(args: Array[AnyValue], call: KernelProcedureCall): Iterator[Array[AnyValue]] = {
    val read = call(args)
    new scala.Iterator[Array[AnyValue]] {
      override def hasNext: Boolean = read.hasNext

      override def next(): Array[AnyValue] = read.next
    }
  }
}
