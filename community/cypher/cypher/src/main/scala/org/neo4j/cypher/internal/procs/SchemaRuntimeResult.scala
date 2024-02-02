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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.EmptyQuerySubscription
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.HeapHighWaterMarkTracker

import java.util

import scala.jdk.CollectionConverters.SetHasAsJava

/**
 * Empty result, as produced by a schema write.
 */
case class SchemaRuntimeResult(
  ctx: QueryContext,
  subscriber: QuerySubscriber,
  runtimeNotifications: Set[InternalNotification]
) extends EmptyQuerySubscription(subscriber) with RuntimeResult {

  override def fieldNames(): Array[String] = Array.empty

  override def queryStatistics(): QueryStatistics = ctx.getOptStatistics.getOrElse(QueryStatistics())

  override def heapHighWaterMark(): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

  override def consumptionState: RuntimeResult.ConsumptionState = ConsumptionState.EXHAUSTED

  override def close(): Unit = {}

  override def queryProfile(): QueryProfile = QueryProfile.NONE

  override def notifications(): util.Set[InternalNotification] = runtimeNotifications.asJava

  override def getErrorOrNull: Throwable = null
}
