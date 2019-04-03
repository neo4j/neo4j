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

import java.util

import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{OperatorProfile, QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.kernel.impl.query.QuerySubscriber

/**
  * Empty result, as produced by a system command.
  */
case class SystemCommandRuntimeResult(ctx: QueryContext, subscriber: QuerySubscriber, execution: InternalExecutionResult) extends RuntimeResult{

  override val fieldNames: Array[String] = execution.fieldNames()
  private var resultRequested = false

  // The signature mode is taking care of eagerization

  override val asIterator: ResourceIterator[util.Map[String, AnyRef]] = execution.javaIterator

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    execution.accept(visitor)
  }

  override def queryStatistics(): QueryStatistics = execution.queryStatistics()

  override def isIterable: Boolean = true

  override def consumptionState: RuntimeResult.ConsumptionState =
    if (!resultRequested) ConsumptionState.NOT_STARTED
    else if (asIterator.hasNext) ConsumptionState.HAS_MORE
    else ConsumptionState.EXHAUSTED

  override def close(): Unit = execution.close()

  override def queryProfile(): QueryProfile = SystemCommandProfile(0)

  override def request(numberOfRecords: Long): Unit = execution.request(numberOfRecords)

  override def cancel(): Unit = execution.cancel()

  override def await(): Boolean = execution.await()
}

case class SystemCommandProfile(rowCount: Long) extends QueryProfile with OperatorProfile {

  override def operatorProfile(operatorId: Int): OperatorProfile = this

  override def time(): Long = OperatorProfile.NO_DATA

  override def dbHits(): Long = 1 // for unclear reasons

  override def rows(): Long = rowCount

  override def pageCacheHits(): Long = OperatorProfile.NO_DATA

  override def pageCacheMisses(): Long = OperatorProfile.NO_DATA
}
