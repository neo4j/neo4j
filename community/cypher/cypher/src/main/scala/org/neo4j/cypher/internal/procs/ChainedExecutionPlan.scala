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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.CountingQueryContext
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.api.query.RuntimeName
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.values.virtual.MapValue

import java.util

import scala.annotation.tailrec
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

  protected def sourcePlan: Option[ExecutionPlan] = source

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
    // The chain is walked iteratively (descent + fold) so that very deep chains
    // (e.g. GRANT MATCH expanded over many properties × labels) do not blow the JVM stack.
    // Only the outermost query is tied into the reactive results stream; inner layers get
    // a simplified counting subscriber via querySubscriber.

    type Layer = (ChainedExecutionPlan[T], T, QuerySubscriber)

    // Walks the chain of ChainedExecutionPlan layers via source pointers.
    // Returns the innermost (deepest) layer separately so it can seed the fold,
    // the remaining layers in outward order, and the optional non-chained terminal source.
    @tailrec
    def descend(
      plan: ChainedExecutionPlan[T],
      origCtx: QueryContext,
      sub: QuerySubscriber,
      outward: List[Layer]
    ): (Layer, List[Layer], Option[ExecutionPlan]) = {
      val ctx = plan.createContext(origCtx)
      plan.sourcePlan match {
        case Some(next: ChainedExecutionPlan[_]) =>
          // All chained plans in a single chain share the same T in practice
          // (AdministrationChainedExecutionPlan uses SystemUpdateCountingQueryContext;
          // SchemaExecutionPlan uses UpdateCountingQueryContext; chains never mix).
          // The original recursive implementation made the same assumption implicitly
          // via the r.ctx.asInstanceOf[T] cast in applyLayer below.
          descend(
            next.asInstanceOf[ChainedExecutionPlan[T]],
            ctx,
            plan.querySubscriber(ctx, sub),
            (plan, ctx, sub) :: outward
          )
        case other =>
          ((plan, ctx, sub), outward, other)
      }
    }

    val (innermost, outwardLayers, nonChainedSource) = descend(this, originalCtx, subscriber, Nil)
    val (innermostPlan, innermostCtx, innermostSubscriber) = innermost

    // Run the innermost non-chained source (if any) once, using the deepest layer's ctx/subscriber.
    val initialResult: Option[RuntimeResult] = nonChainedSource.map { src =>
      src.run(
        innermostCtx,
        executionMode,
        params,
        prePopulateResults,
        ignore,
        innermostPlan.querySubscriber(innermostCtx, innermostSubscriber)
      )
    }

    def applyLayer(plan: ChainedExecutionPlan[T], ctx: T, sub: QuerySubscriber, prev: RuntimeResult): RuntimeResult =
      prev match {
        case ir: IgnoredRuntimeResult =>
          plan.onSkip(ctx, sub, ir.runtimeNotifications)
        case r: UpdatingSystemCommandRuntimeResult =>
          plan.runSpecific(
            r.ctx.asInstanceOf[T],
            executionMode,
            params,
            prePopulateResults,
            sub,
            r.notifications().asScala.toSet
          )
        case r =>
          plan.runSpecific(ctx, executionMode, params, prePopulateResults, sub, r.notifications.asScala.toSet)
      }

    // Process the innermost layer first; this is the only place a None previous result is possible
    // (when there is no non-chained source at the leaf), so we collapse it to the empty-notifications
    // call here and then fold the remaining layers with a guaranteed RuntimeResult accumulator.
    val innermostResult: RuntimeResult = initialResult match {
      case Some(r) => applyLayer(innermostPlan, innermostCtx, innermostSubscriber, r)
      case None =>
        innermostPlan.runSpecific(
          innermostCtx,
          executionMode,
          params,
          prePopulateResults,
          innermostSubscriber,
          Set.empty
        )
    }

    // Unwind from the next-innermost layer out to the outermost.
    outwardLayers.foldLeft(innermostResult) {
      case (prev, (plan, ctx, sub)) => applyLayer(plan, ctx, sub, prev)
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

  override def createContext(originalCtx: QueryContext): SystemUpdateCountingQueryContext =
    SystemUpdateCountingQueryContext.from(originalCtx)

  override def querySubscriber(context: SystemUpdateCountingQueryContext, qs: QuerySubscriber): QuerySubscriber =
    new QuerySubscriberAdapter() {

      override def onResultCompleted(statistics: QueryStatistics): Unit =
        if (statistics.containsUpdates()) context.systemUpdates.increase()
    }

  override def runtimeName: RuntimeName = RuntimeName.SYSTEM
}

object AdministrationChainedExecutionPlan {

  // To avoid code generation for administration commands
  // also set the runtime to not rely on default (to avoid being blocked if default is set to parallel runtime as it doesn't allow updates)
  // Slotted is picked as it is available in both Community and Enterprise, and handles all kinds of commands.
  // interpretedPipesFallback is pinned to default; non-default values would be rejected against runtime=slotted.
  final private val codeGenerationPreParserOptions: String =
    "operatorEngine=interpreted expressionEngine=interpreted runtime=slotted interpretedPipesFallback=default"

  def formatQuery(cypher: String, cypherVersion: CypherVersion): String = {
    s"CYPHER $cypherVersion $codeGenerationPreParserOptions $cypher"
  }
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
