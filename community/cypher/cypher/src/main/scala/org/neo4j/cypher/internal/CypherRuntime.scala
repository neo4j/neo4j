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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.RuntimeUnsupportedException
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.kernel.api.AssertOpen
import org.neo4j.logging.InternalLog
import org.neo4j.notifications.RuntimeUnsupportedNotification

import java.time.Clock

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
   * @return the executable plan
   */
  def compileToExecutable(logicalQuery: LogicalQuery, context: CONTEXT): ExecutionPlan

  def name: String

  def correspondingRuntimeOption: Option[CypherRuntimeOption]
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
 * @param effectiveCardinalities effective cardinalities (estimated rows when considering selectivity imposed by a limit) of all operators in the logical plan tree
 * @param providedOrders provided order of all operators in the logical plan tree
 * @param leveragedOrders leveragedOrder of all operators in the logical plan tree
 * @param hasLoadCSV a flag showing if the query contains a load csv, used for tracking line numbers
 * @param doProfile `true` if a profiling query otherwise `false`
 */
case class LogicalQuery(
  logicalPlan: LogicalPlan,
  queryText: String,
  readOnly: Boolean,
  resultColumns: Array[String],
  semanticTable: SemanticTable,
  effectiveCardinalities: EffectiveCardinalities,
  providedOrders: ProvidedOrders,
  leveragedOrders: LeveragedOrders,
  hasLoadCSV: Boolean,
  idGen: IdGen,
  doProfile: Boolean
)

/**
 * Context in which the Runtime performs physical planning
 */
abstract class RuntimeContext {
  def cypherVersion: CypherVersion
  def tokenContext: ReadTokenContext
  def schemaRead: SchemaRead
  def procedures: Procedures
  def config: CypherRuntimeConfiguration
  def compileExpressions: Boolean
  def log: InternalLog
  def anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  def materializedEntitiesMode: Boolean
  def isCommunity: Boolean

  def assertOpen: AssertOpen
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
  def create(
    cypherVersion: CypherVersion,
    tokenContext: ReadTokenContext,
    schemaRead: SchemaRead,
    procedures: Procedures,
    clock: Clock,
    debugOptions: CypherDebugOptions,
    compileExpressions: Boolean,
    materializedEntitiesMode: Boolean,
    operatorEngine: CypherOperatorEngineOption,
    interpretedPipesFallback: CypherInterpretedPipesFallbackOption,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    assertOpen: AssertOpen
  ): CONTEXT

  def config: CypherRuntimeConfiguration

  /**
   * Assert that all acquired resources have been released back to their central pools.
   *
   * Examples of such resources would be worker threads and [[org.neo4j.internal.kernel.api.Cursor]]
   */
  @throws[RuntimeResourceLeakException]
  def assertAllReleased(): Unit

  /**
   * Wait until all query workers have completed ongoing work according to current demand and settled down in an idle state
   *
   * @param timeoutMs Timeout in ms
   * @return true if all workers settled in an idle state, or false if the timeout occurred
   */
  def waitForWorkersToIdle(timeoutMs: Int): Boolean
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

  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = None

  override def compileToExecutable(logicalQuery: LogicalQuery, context: RuntimeContext): ExecutionPlan =
    throw new CantCompileQueryException(
      s"This version of Neo4j does not support the requested runtime: `$requestedRuntime`"
    )
}

/**
 * Composite cypher runtime, which attempts to compile using several different runtimes before giving up.
 *
 * @param runtimes the runtimes to attempt to compile with, in order of priority
 * @param requestedRuntime the requested runtime, used to provide error messages
 */
class FallbackRuntime[CONTEXT <: RuntimeContext](
  runtimes: IndexedSeq[CypherRuntime[CONTEXT]],
  requestedRuntime: CypherRuntimeOption
) extends CypherRuntime[CONTEXT] {

  override def name: String = "fallback"

  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = runtimes.collectFirst {
    case r if r.correspondingRuntimeOption.isDefined => r.correspondingRuntimeOption.get
  }

  private def publicCannotCompile(originalException: Exception) = {
    throw new RuntimeUnsupportedException(originalException.getMessage, originalException)
  }

  override def compileToExecutable(logicalQuery: LogicalQuery, context: CONTEXT): ExecutionPlan = {
    val logger = new RecordingNotificationLogger()

    var i = 0
    var failedAttempt: (CypherRuntime[CONTEXT], String) = null
    var lastException: Exception = null
    while (i < runtimes.length) {
      val runtime = runtimes(i)

      if (failedAttempt != null && runtime != SchemaCommandRuntime) {
        val (failingRuntime, message) = failedAttempt
        logger.log(RuntimeUnsupportedNotification(runtimeConf(failingRuntime), runtimeConf(runtime), message))
        failedAttempt = null
      }

      try {
        val plan = runtime.compileToExecutable(logicalQuery, context)
        val notifications = logger.notifications
        val notifiedPlan = if (notifications.isEmpty) plan else ExecutionPlanWithNotifications(plan, notifications)
        return notifiedPlan
      } catch {
        case e: CantCompileQueryException =>
          lastException = e

          if (
            runtime != SchemaCommandRuntime &&
            requestedRuntime != CypherRuntimeOption.default &&
            i < runtimes.length - 1
          ) {
            failedAttempt = (runtime, e.getMessage)
          }
        case e: RuntimeUnsupportedException =>
          // this is an explicit failure that we shouldn't try to fallback from
          throw e
        case e: Exception =>
          lastException = e
          // That is unexpected. Let's log, but continue trying other runtimes
          context.log.debug(
            s"Runtime ${runtime.getClass.getSimpleName} failed to compile query ${logicalQuery.queryText}",
            e
          )
      }
      i += 1
    }
    // All runtimes failed
    lastException match {
      case e: CantCompileQueryException =>
        throw publicCannotCompile(e)
      case e =>
        throw e

    }
  }

  private def runtimeConf(runtime: CypherRuntime[CONTEXT]): String = {
    runtime match {
      case UnknownRuntime(requestedRuntime) => s"runtime=$requestedRuntime"
      case _ => runtime.correspondingRuntimeOption.map(_.renderExplicit).getOrElse(s"runtime=${runtime.name}")
    }
  }
}

case class ExecutionPlanWithNotifications(inner: ExecutionPlan, extraNotifications: Set[InternalNotification])
    extends DelegatingExecutionPlan(inner) {

  override def notifications: Set[InternalNotification] = inner.notifications ++ extraNotifications
}
