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
import org.neo4j.cypher.internal.v3_5.logical.plans.QualifiedName
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.{ProcedureCallContext, UserAggregator, QualifiedName => KernelQualifiedName}
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.AnyValue

import scala.collection.JavaConverters._

/**
  * This class contains helpers for calling procedures, user-defined functions and user-defined aggregations.
  */
object CallSupport {

  type KernelProcedureCall = (Array[AnyRef]) => RawIterator[Array[AnyRef], ProcedureException]

  def callFunction(transactionalContext: TransactionalContext, id: Int, args: Seq[AnyValue],
                   allowed: Array[String]): AnyValue = {
    if (shouldElevate(transactionalContext, allowed))
      transactionalContext.kernelTransaction().procedures().functionCallOverride(id, args.toArray)
    else
      transactionalContext.kernelTransaction().procedures().functionCall(id, args.toArray)
  }

  def callFunction(transactionalContext: TransactionalContext, name: QualifiedName, args: Seq[AnyValue],
                   allowed: Array[String]): AnyValue = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    if (shouldElevate(transactionalContext, allowed))
      transactionalContext.kernelTransaction().procedures().functionCallOverride(kn, args.toArray)
    else
      transactionalContext.kernelTransaction().procedures().functionCall(kn, args.toArray)
  }

  def callReadOnlyProcedure(transactionalContext: TransactionalContext, id: Int, args: Seq[Any],
                            allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyRef]] = {
    val call: KernelProcedureCall =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction.procedures().procedureCallReadOverride(id, _, context)
      else
        transactionalContext.kernelTransaction.procedures().procedureCallRead(id, _, context)

    callProcedure(args, call)
  }

  def callReadWriteProcedure(transactionalContext: TransactionalContext, id: Int, args: Seq[Any],
                             allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyRef]] = {
    val call: KernelProcedureCall =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().procedureCallWriteOverride(id, _, context)
      else
        transactionalContext.kernelTransaction().procedures().procedureCallWrite(id, _, context)
    callProcedure(args, call)
  }

  def callSchemaWriteProcedure(transactionalContext: TransactionalContext, id: Int, args: Seq[Any],
                               allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyRef]] = {
    val call: KernelProcedureCall =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().procedureCallSchemaOverride(id, _, context)
      else
        transactionalContext.kernelTransaction().procedures().procedureCallSchema(id, _, context)
    callProcedure(args, call)
  }

  def callDbmsProcedure(transactionalContext: TransactionalContext, id: Int, args: Seq[Any], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyRef]] =
    callProcedure(args,
                  transactionalContext.dbmsOperations.procedureCallDbms(id,
                                                                        _,
                                                                        transactionalContext.graph
                                                                          .getDependencyResolver,
                                                                        transactionalContext.securityContext,
                                                                        transactionalContext.resourceTracker,
                                                                        context))

  def callReadOnlyProcedure(transactionalContext: TransactionalContext, name: QualifiedName, args: Seq[Any],
                            allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyRef]] = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val call: KernelProcedureCall =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().procedureCallReadOverride(kn, _, context)
      else
        transactionalContext.kernelTransaction().procedures().procedureCallRead(kn, _, context)

    callProcedure(args, call)
  }

  def callReadWriteProcedure(transactionalContext: TransactionalContext, name: QualifiedName, args: Seq[Any],
                             allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyRef]] = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val call: KernelProcedureCall =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().procedureCallWriteOverride(kn, _, context)
      else
        transactionalContext.kernelTransaction().procedures().procedureCallWrite(kn, _, context)
    callProcedure(args, call)
  }

  def callSchemaWriteProcedure(transactionalContext: TransactionalContext, name: QualifiedName, args: Seq[Any],
                               allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyRef]] = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val call: KernelProcedureCall =
      if (shouldElevate(transactionalContext: TransactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().procedureCallSchemaOverride(kn, _, context)
      else
        transactionalContext.kernelTransaction().procedures().procedureCallSchema(kn, _, context)
    callProcedure(args, call)
  }

  def callDbmsProcedure(transactionalContext: TransactionalContext, name: QualifiedName, args: Seq[Any],
                        allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyRef]] = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    callProcedure(args,
                  transactionalContext.dbmsOperations.procedureCallDbms(kn,
                                                                        _,
                                                                        transactionalContext.graph
                                                                          .getDependencyResolver,
                                                                        transactionalContext.securityContext,
                                                                        transactionalContext.resourceTracker,
                                                                        context))
  }

  def aggregateFunction(transactionalContext: TransactionalContext, id: Int, allowed: Array[String]): UserDefinedAggregator = {
    val aggregator: UserAggregator =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().aggregationFunctionOverride(id)
      else
        transactionalContext.kernelTransaction().procedures().aggregationFunction(id)

    userDefinedAggregator(aggregator)
  }

  def aggregateFunction(transactionalContext: TransactionalContext, name: QualifiedName, allowed: Array[String]): UserDefinedAggregator = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val aggregator: UserAggregator =
      if (shouldElevate(transactionalContext, allowed))
        transactionalContext.kernelTransaction().procedures().aggregationFunctionOverride(kn)
      else
        transactionalContext.kernelTransaction().procedures().aggregationFunction(kn)

    userDefinedAggregator(aggregator)
  }

  private def callProcedure(args: Seq[Any], call: KernelProcedureCall): Iterator[Array[AnyRef]] = {
    val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
    val read = call(toArray)
    new scala.Iterator[Array[AnyRef]] {
      override def hasNext: Boolean = read.hasNext

      override def next(): Array[AnyRef] = read.next
    }
  }

  private def userDefinedAggregator(aggregator: UserAggregator): UserDefinedAggregator = {
    new UserDefinedAggregator {
      override def result: AnyRef = aggregator.result()

      override def update(args: IndexedSeq[Any]): Unit = {
        val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
        aggregator.update(toArray)
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
