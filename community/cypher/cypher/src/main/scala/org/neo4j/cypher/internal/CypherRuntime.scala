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
package org.neo4j.cypher.internal

import java.io.File
import java.time.Clock

import org.neo4j.cypher.internal.compiler.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders}
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.MemoryTrackingController
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.frontend.phases.RecordingNotificationLogger
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.{CypherInterpretedPipesFallbackOption, CypherOperatorEngineOption, CypherRuntimeOption}
import org.neo4j.exceptions.{CantCompileQueryException, RuntimeUnsupportedException}
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.{Cursor, SchemaRead}
import org.neo4j.logging.Log
import org.neo4j.util.Preconditions

/**
  * A cypher runtime. Compiles logical plans into a executable form, which can
  * be used directly to serve the query.
  */
trait CypherRuntime[-CONTEXT <: RuntimeContext] {

  /**
    * Compile a logical plan to an executable plan.
    *
    * WARNING: This code path is in the middle of a refactor and will be modified, changed and reworked.
    *
    * @param logicalQuery the logical query to compile
    * @param context the compilation context
    * @param securityContext the security context for the current user
    * @return the executable plan
    */
  def compileToExecutable(logicalQuery: LogicalQuery, context: CONTEXT, securityContext: SecurityContext = null): ExecutionPlan

  def name: String
}

/**
  * LogicalQuery contains all information about a planned query that is need for a runtime
  * to compile it to a ExecutionPlan.
  *
  * @param logicalPlan the logical plan
  * @param queryText the text representation of the query (only for debugging)
  * @param readOnly true if the query is read only
  * @param resultColumns names of the returned result columns
  * @param semanticTable semantic table with type information on the expressions in the query
  * @param cardinalities cardinalities (estimated rows) of all operators in the logical plan tree
  * @param providedOrders provided order of all operators in the logical plan tree
  * @param hasLoadCSV a flag showing if the query contains a load csv, used for tracking line numbers
  * @param periodicCommitInfo periodic commit info if relevant
  */
case class LogicalQuery(logicalPlan: LogicalPlan,
                        queryText: String,
                        readOnly: Boolean,
                        resultColumns: Array[String],
                        semanticTable: SemanticTable,
                        cardinalities: Cardinalities,
                        providedOrders: ProvidedOrders,
                        hasLoadCSV: Boolean,
                        periodicCommitInfo: Option[PeriodicCommitInfo])

/**
  * Context in which the Runtime performs physical planning
  */
abstract class RuntimeContext {
  def tokenContext: TokenContext
  def schemaRead: SchemaRead
  def config: CypherRuntimeConfiguration
  def compileExpressions: Boolean
  def log: Log
}

/**
  * Manager of runtime contexts.
  *
  * @tparam CONTEXT type of runtime context managed
  */
trait RuntimeContextManager[+CONTEXT <: RuntimeContext] {

  /**
    * Create a new runtime context.
    */
  def create(tokenContext: TokenContext,
             schemaRead: SchemaRead,
             clock: Clock,
             debugOptions: Set[String],
             compileExpressions: Boolean,
             materializedEntitiesMode: Boolean,
             operatorEngine: CypherOperatorEngineOption,
             interpretedPipesFallback: CypherInterpretedPipesFallbackOption
            ): CONTEXT

  /**
    * Assert that all acquired resources have been released back to their central pools.
    *
    * Examples of such resources would be worker threads and [[Cursor]]
    */
  @throws[RuntimeResourceLeakException]
  def assertAllReleased(): Unit
}

/**
  * Exception thrown by [[RuntimeContextManager.assertAllReleased()]] if some resource is
  * found not to be released.
  */
class RuntimeResourceLeakException(msg: String) extends IllegalStateException(msg)

/**
  * Cypher runtime representing a user-selected runtime which is not supported.
  */
case class UnknownRuntime(requestedRuntime: String) extends CypherRuntime[RuntimeContext] {
  override def name: String = "unknown"

  override def compileToExecutable(logicalQuery: LogicalQuery, context: RuntimeContext, securityContext: SecurityContext): ExecutionPlan =
    throw new CantCompileQueryException(s"This version of Neo4j does not support requested runtime: $requestedRuntime")
}

/**
  * Composite cypher runtime, which attempts to compile using several different runtimes before giving up.
  *
  * @param runtimes the runtimes to attempt to compile with, in order of priority
  * @param requestedRuntime the requested runtime, used to provide error messages
  */
class FallbackRuntime[CONTEXT <: RuntimeContext](runtimes: Seq[CypherRuntime[CONTEXT]],
                                                 requestedRuntime: CypherRuntimeOption) extends CypherRuntime[CONTEXT] {

  override def name: String = "fallback"

  private def publicCannotCompile(originalException: Exception) = {
    throw new RuntimeUnsupportedException(originalException.getMessage, originalException)
  }

  override def compileToExecutable(logicalQuery: LogicalQuery, context: CONTEXT, securityContext: SecurityContext): ExecutionPlan = {
    val logger = new RecordingNotificationLogger()

    var i = 0
    var lastException: Exception = null
    while (i < runtimes.length) {
      val runtime = runtimes(i)

      try {
        val plan = runtime.compileToExecutable(logicalQuery, context)
        val notifications = logger.notifications
        val notifiedPlan = if (notifications.isEmpty) plan else ExecutionPlanWithNotifications(plan, notifications)
       return notifiedPlan
      } catch {
        case e: CantCompileQueryException =>
          lastException = e
          if (runtime != SchemaCommandRuntime && requestedRuntime != CypherRuntimeOption.default) {
            logger.log(RuntimeUnsupportedNotification(e.getMessage))
          }
        case e: Exception =>
          lastException = e
          // That is unexpected. Let's log, but continue trying other runtimes
          context.log.debug(s"Runtime ${runtime.getClass.getSimpleName} failed to compile query ${logicalQuery.queryText}", e)
      }
      i += 1
    }
    // All runtimes failed
    lastException match {
      case e:CantCompileQueryException =>
        throw publicCannotCompile(e)
      case e =>
        throw e

    }
  }
}

case class CypherRuntimeConfiguration(workers: Int,
                                      pipelinedBatchSizeSmall: Int,
                                      pipelinedBatchSizeBig: Int,
                                      schedulerTracing: SchedulerTracingConfiguration,
                                      lenientCreateRelationship: Boolean,
                                      memoryTrackingController: MemoryTrackingController,
                                      enableMonitors: Boolean) {

  Preconditions.checkArgument(pipelinedBatchSizeSmall <= pipelinedBatchSizeBig, s"pipelinedBatchSizeSmall (got $pipelinedBatchSizeSmall) must be <= pipelinedBatchSizeBig (got $pipelinedBatchSizeBig)")
}

sealed trait SchedulerTracingConfiguration
case object NoSchedulerTracing extends SchedulerTracingConfiguration
case object StdOutSchedulerTracing extends SchedulerTracingConfiguration
case class FileSchedulerTracing(file: File) extends SchedulerTracingConfiguration

case class ExecutionPlanWithNotifications(inner: ExecutionPlan, extraNotifications: Set[InternalNotification]) extends DelegatingExecutionPlan(inner) {

  override def notifications: Set[InternalNotification] = inner.notifications ++ extraNotifications
}
