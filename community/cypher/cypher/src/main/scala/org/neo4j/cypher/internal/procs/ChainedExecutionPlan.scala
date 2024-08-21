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

import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.SystemCommandRuntimeName
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.CountingQueryContext
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.values.virtual.MapValue

import java.util

import scala.jdk.CollectionConverters.SetHasAsJava
import scala.jdk.CollectionConverters.SetHasAsScala

/**
 * System commands are broken down into a linear chain of sub-commands. The outermost (or last) command
 * will be passed the original QuerySubscriber (coming from the BOLT server) and this can be tied into
 * the reactive-results system. For this reason it is important to make sure that outer subscriber is
 * treated correctly for reactive results. The inner commands can instead be passed simple QuerySubscribers
 * that either do nothing or simply track the existence of database changes in order to keep a count of inner
 * commands.
 */
abstract class ChainedExecutionPlan[T <: QueryContext with CountingQueryContext](source: Option[ExecutionPlan])
    extends ExecutionPlan {

  def runSpecific(
    ctx: T,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    subscriber: QuerySubscriber,
    previousNotifications: Set[InternalNotification]
  ): RuntimeResult

  def createContext(originalCtx: QueryContext): T
  def querySubscriber(context: T, subscriber: QuerySubscriber): QuerySubscriber

  override def run(
    originalCtx: QueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    ignore: InputDataStream,
    subscriber: QuerySubscriber
  ): RuntimeResult = {
    val ctx = createContext(originalCtx)
    // Only the outermost query should be tied into the reactive results stream. The source queries use a simplified counting subscriber
    val sourceResult =
      source.map(_.run(ctx, executionMode, params, prePopulateResults, ignore, querySubscriber(ctx, subscriber)))
    sourceResult match {
      case Some(i: IgnoredRuntimeResult) =>
        onSkip(ctx, subscriber, i.runtimeNotifications)
      case Some(UpdatingSystemCommandRuntimeResult(newCtx, runtimeNotifications)) =>
        runSpecific(
          newCtx.asInstanceOf[T],
          executionMode,
          params,
          prePopulateResults,
          subscriber,
          runtimeNotifications
        )
      case Some(r: RuntimeResult) =>
        runSpecific(ctx, executionMode, params, prePopulateResults, subscriber, r.notifications.asScala.toSet)
      case _ =>
        runSpecific(ctx, executionMode, params, prePopulateResults, subscriber, Set.empty)
    }
  }

  def onSkip(ctx: T, subscriber: QuerySubscriber, runtimeNotifications: Set[InternalNotification]): RuntimeResult = {
    // When an operation in the chain switches the entire chain to ignore mode we still need to notify the outer most subscriber
    // This is a no-op for all elements of the chain except the last (outermost) which will be the BoltAdapterSubscriber
    subscriber.onResultCompleted(ctx.getStatistics)
    IgnoredRuntimeResult(runtimeNotifications)
  }

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}

abstract class AdministrationChainedExecutionPlan(source: Option[ExecutionPlan])
    extends ChainedExecutionPlan[SystemUpdateCountingQueryContext](source) {
  // To avoid code generation for administration commands
  protected val queryPrefix: String = "CYPHER operatorEngine=interpreted expressionEngine=interpreted "

  override def createContext(originalCtx: QueryContext): SystemUpdateCountingQueryContext =
    SystemUpdateCountingQueryContext.from(originalCtx)

  override def querySubscriber(context: SystemUpdateCountingQueryContext, qs: QuerySubscriber): QuerySubscriber =
    new QuerySubscriberAdapter() {

      override def onResultCompleted(statistics: QueryStatistics): Unit =
        if (statistics.containsUpdates()) context.systemUpdates.increase()
    }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName
}

case class IgnoredRuntimeResult(runtimeNotifications: Set[InternalNotification]) extends RuntimeResult {
  import org.neo4j.cypher.internal.runtime.QueryStatistics

  override def hasServedRows: Boolean = false
  override def fieldNames(): Array[String] = Array.empty
  override def queryStatistics(): QueryStatistics = QueryStatistics()
  override def heapHighWaterMark(): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED
  override def consumptionState: RuntimeResult.ConsumptionState = ConsumptionState.EXHAUSTED
  override def close(): Unit = {}
  override def queryProfile(): QueryProfile = QueryProfile.NONE
  override def request(numberOfRecords: Long): Unit = {}
  override def cancel(): Unit = {}
  override def await(): Boolean = false
  override def notifications(): util.Set[InternalNotification] = runtimeNotifications.asJava
  override def getErrorOrNull: Throwable = null
}

case object IgnoredRuntimeResult {
  def apply(): IgnoredRuntimeResult = IgnoredRuntimeResult(Set.empty)
}
