/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.runtime.UserDefinedAggregator
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.{ProcedureCallContext, UserAggregator}
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.AnyValue

/**
  * This class contains helpers for calling procedures, user-defined functions and user-defined aggregations.
  */
object CallSupport {

  type KernelProcedureCall = Array[AnyValue] => RawIterator[Array[AnyValue], ProcedureException]

  def callFunction(transactionalContext: TransactionalContext, id: Int, args: Array[AnyValue],
                   allowed: Array[String]): AnyValue = {
    if (shouldElevate(transactionalContext, allowed))
      transactionalContext.kernelTransaction().procedures().functionCallOverride(id, args)
    else
      transactionalContext.kernelTransaction().procedures().functionCall(id, args)
  }

  def callReadOnlyProcedure(transactionalContext: TransactionalContext, id: Int, args: Seq[AnyValue],
                            allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] = {
    val call: KernelProcedureCall =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction.procedures().procedureCallReadOverride(id, _, context)
      else
        transactionalContext.kernelTransaction.procedures().procedureCallRead(id, _, context)

    callProcedure(args, call)
  }

  def callReadWriteProcedure(transactionalContext: TransactionalContext, id: Int, args: Seq[AnyValue],
                             allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] = {
    val call: KernelProcedureCall =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().procedureCallWriteOverride(id, _, context)
      else
        transactionalContext.kernelTransaction().procedures().procedureCallWrite(id, _, context)
    callProcedure(args, call)
  }

  def callSchemaWriteProcedure(transactionalContext: TransactionalContext, id: Int, args: Seq[AnyValue],
                               allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] = {
    val call: KernelProcedureCall =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().procedureCallSchemaOverride(id, _, context)
      else
        transactionalContext.kernelTransaction().procedures().procedureCallSchema(id, _, context)
    callProcedure(args, call)
  }

  def callDbmsProcedure(transactionalContext: TransactionalContext, id: Int, args: Seq[AnyValue],
                        allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    callProcedure(args,
                  transactionalContext.dbmsOperations.procedureCallDbms(id,
                                                                        _,
                                                                        transactionalContext.transaction(),
                                                                        transactionalContext.graph.getDependencyResolver,
                                                                        transactionalContext.securityContext,
                                                                        transactionalContext.resourceTracker,
                                                                        transactionalContext.valueMapper))

  def aggregateFunction(transactionalContext: TransactionalContext, id: Int, allowed: Array[String]): UserDefinedAggregator = {
    val aggregator: UserAggregator =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().aggregationFunctionOverride(id)
      else
        transactionalContext.kernelTransaction().procedures().aggregationFunction(id)

    userDefinedAggregator(aggregator)
  }

  private def callProcedure(args: Seq[AnyValue], call: KernelProcedureCall): Iterator[Array[AnyValue]] = {
    val read = call(args.toArray)
    new scala.Iterator[Array[AnyValue]] {
      override def hasNext: Boolean = read.hasNext

      override def next(): Array[AnyValue] = read.next
    }
  }

  private def userDefinedAggregator(aggregator: UserAggregator): UserDefinedAggregator = {
    new UserDefinedAggregator {
      override def result: AnyValue = aggregator.result()

      override def update(args: IndexedSeq[AnyValue]): Unit = {
        aggregator.update(args.toArray)
      }
    }
  }

  private def shouldElevate(transactionalContext: TransactionalContext, allowed: Array[String]): Boolean = {
    // We have to be careful with elevation, since we cannot elevate permissions in a nested procedure call
    // above the original allowed procedure mode. We enforce this by checking if mode is already an overridden mode.
    val accessMode = transactionalContext.securityContext.mode()
    allowed.nonEmpty && !accessMode.isOverridden && accessMode.allowsProcedureWith(allowed)
  }
}
