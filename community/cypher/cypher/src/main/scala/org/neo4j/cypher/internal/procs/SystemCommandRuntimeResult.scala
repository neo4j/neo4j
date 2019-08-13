/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.procs

import java.{lang, util}
import java.util.Optional

import org.neo4j.cypher.internal.result.{Error, InternalExecutionResult}
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics}
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{OperatorProfile, QueryProfile, RuntimeResult}
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.KernelTransaction

/**
  * Results, as produced by a system command.
  */
case class SystemCommandRuntimeResult(ctx: SystemUpdateCountingQueryContext,
                                      execution: SystemCommandExecutionResult,
                                      subscriber: SystemCommandQuerySubscriber,
                                      securityContext: SecurityContext,
                                      kernelTransaction: KernelTransaction) extends RuntimeResult {

  override val fieldNames: Array[String] = execution.fieldNames()
  private var state = ConsumptionState.NOT_STARTED

  override def queryStatistics(): QueryStatistics = ctx.getOptStatistics.getOrElse(QueryStatistics())

  override def totalAllocatedMemory(): Optional[lang.Long] = Optional.empty()

  override def consumptionState: RuntimeResult.ConsumptionState = state

  override def close(): Unit = execution.inner.close()

  override def queryProfile(): QueryProfile = SystemCommandProfile(0)

  override def request(numberOfRecords: Long): Unit = {
    var revertSecurityContextChange: KernelTransaction.Revertable = null
    try {
      revertSecurityContextChange = kernelTransaction.overrideWith(securityContext)

      state = ConsumptionState.HAS_MORE
      execution.inner.request(numberOfRecords)
      // The lower level (execution) is capturing exceptions using the subscriber, but this level is expecting to do the same higher up, so re-throw to trigger that code path
      subscriber.assertNotFailed(e => execution.inner.close(Error(e)))
    } finally {
      if (revertSecurityContextChange != null) revertSecurityContextChange
    }
  }

  override def cancel(): Unit = execution.inner.cancel()

  override def await(): Boolean = {
    val hasMore = execution.inner.await()
    if (!hasMore) {
      state = ConsumptionState.EXHAUSTED
    }
    hasMore
  }
}

class SystemCommandExecutionResult(val inner: InternalExecutionResult) {
  def fieldNames(): Array[String] = inner.fieldNames()
}

class ColumnMappingSystemCommandExecutionResult(context: QueryContext,
                                                inner: InternalExecutionResult,
                                                ignore: Seq[String] = Seq.empty,
                                                valueExtractor: (String, util.Map[String, AnyRef]) => AnyRef = (k, r) => r.get(k))
  extends SystemCommandExecutionResult(inner) {

  self =>

  private val innerFields = inner.fieldNames()
  //private val ignoreIndexes = innerFields.zipWithIndex.filter(v => ignore.contains(v._1)).map(_._2)
  override val fieldNames: Array[String] = innerFields.filter(!ignore.contains(_))
}

case class SystemCommandProfile(rowCount: Long) extends QueryProfile with OperatorProfile {

  override def operatorProfile(operatorId: Int): OperatorProfile = this

  override def time(): Long = OperatorProfile.NO_DATA

  override def dbHits(): Long = 1 // for unclear reasons

  override def rows(): Long = rowCount

  override def pageCacheHits(): Long = OperatorProfile.NO_DATA

  override def pageCacheMisses(): Long = OperatorProfile.NO_DATA
}
