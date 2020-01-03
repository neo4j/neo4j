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
package org.neo4j.cypher.internal.compatibility

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{DelegatingExecutionPlan, ExecutionPlan}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_5.{CypherPlannerConfiguration, RuntimeUnsupportedNotification}
import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.v3_5.frontend.phases.RecordingNotificationLogger
import org.neo4j.cypher.internal.v3_5.util.InternalNotification
import org.neo4j.cypher.{CypherRuntimeOption, InvalidArgumentException, exceptionHandler}
import org.neo4j.logging.Log

import scala.concurrent.duration.Duration

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
    * @param logicalPlan the logical plan to compile
    * @param context the compilation context
    * @return the executable plan
    */
  def compileToExecutable(logicalPlan: LogicalPlanState, context: CONTEXT): ExecutionPlan
}

/**
  * Context in which the Runtime performs physical planning
  */
abstract class RuntimeContext {
  def tokenContext: TokenContext
  def readOnly: Boolean
  def config: CypherPlannerConfiguration
  def compileExpressions: Boolean
  def log: Log
}

/**
  * Creator of runtime contexts.
  *
  * @tparam CONTEXT type of runtime context created
  */
trait RuntimeContextCreator[CONTEXT <: RuntimeContext] {
  def create(tokenContext: TokenContext,
             clock: Clock,
             debugOptions: Set[String],
             readOnly: Boolean,
             compileExpressions: Boolean
            ): CONTEXT
}

/**
  * Cypher runtime representing a user-selected runtime which is not supported.
  */
object UnknownRuntime extends CypherRuntime[RuntimeContext] {
  def compileToExecutable(logicalPlan: LogicalPlanState, context: RuntimeContext): ExecutionPlan =
    throw new CantCompileQueryException()
}

/**
  * Composite cypher runtime, which attempts to compile using several different runtimes before giving up.
  *
  * @param runtimes the runtimes to attempt to compile with, in order of priority
  * @param requestedRuntime the requested runtime, used to provide error messages
  */
class FallbackRuntime[CONTEXT <: RuntimeContext](runtimes: Seq[CypherRuntime[CONTEXT]],
                                                 requestedRuntime: CypherRuntimeOption) extends CypherRuntime[CONTEXT] {

  private def publicCannotCompile(originalException: Exception) =
    {
      val message = s"This version of Neo4j does not support requested runtime: ${requestedRuntime.name}"
      val invalidArgument = new InvalidArgumentException(message, originalException)
      new org.neo4j.graphdb.QueryExecutionException(message, invalidArgument, invalidArgument.status.code().serialize())
    }

  override def compileToExecutable(logicalPlan: LogicalPlanState, context: CONTEXT): ExecutionPlan = {
    val logger = new RecordingNotificationLogger()

    var i = 0
    var lastException: Exception = null
    while (i < runtimes.length) {
      val runtime = runtimes(i)

      try {
        val plan = exceptionHandler.runSafely(runtime.compileToExecutable(logicalPlan, context))
        val notifications = logger.notifications
        val notifiedPlan = if (notifications.isEmpty) plan else ExecutionPlanWithNotifications(plan, notifications)
       return notifiedPlan
      } catch {
        case e: CantCompileQueryException =>
          lastException = e
          if (runtime != ProcedureCallOrSchemaCommandRuntime && requestedRuntime != CypherRuntimeOption.default) {
            logger.log(RuntimeUnsupportedNotification)
          }
        case e: Exception =>
          lastException = e
          // That is unexpected. Let's log, but continue trying other runtimes
          context.log.debug(s"Runtime ${runtime.getClass.getSimpleName} failed to compile query ${logicalPlan.queryText}", e)
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
                                      morselSize: Int,
                                      doSchedulerTracing: Boolean,
                                      waitTimeout: Duration)

case class ExecutionPlanWithNotifications(inner: ExecutionPlan, extraNotifications: Set[InternalNotification]) extends DelegatingExecutionPlan(inner) {

  override def notifications: Set[InternalNotification] = inner.notifications ++ extraNotifications
}
