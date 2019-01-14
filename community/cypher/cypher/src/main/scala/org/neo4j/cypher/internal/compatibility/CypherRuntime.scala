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
package org.neo4j.cypher.internal.compatibility

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.RuntimeName
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{DelegatingExecutionPlan, ExecutionPlan}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_5.{CypherPlannerConfiguration, RuntimeUnsupportedNotification}
import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.{CypherRuntimeOption, InvalidArgumentException, exceptionHandler}
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.frontend.phases.{InternalNotificationLogger, RecordingNotificationLogger}
import org.neo4j.cypher.internal.v3_5.util.InternalNotification

import scala.concurrent.duration.Duration
import scala.util.Try

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
  * In addition to attempting the provided runtimes, this runtime allways first attempt to compile using
  * `org.neo4j.cypher.internal.compatibility.ProcedureCallOrSchemaCommandRuntime`, in case the query
  * is a simple procedure call of schema command.
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
    var executionPlan: Try[ExecutionPlan] = Try(ProcedureCallOrSchemaCommandRuntime.compileToExecutable(logicalPlan, context))
    val logger = new RecordingNotificationLogger()
    for (runtime <- runtimes if executionPlan.isFailure) {
      executionPlan =
        Try(
          exceptionHandler.runSafely(
            runtime.compileToExecutable(logicalPlan, context)
          )
        )

      if (executionPlan.isFailure && requestedRuntime != CypherRuntimeOption.default)
        logger.log(RuntimeUnsupportedNotification)
    }
    val notifications = logger.notifications

    val plan = executionPlan.recover({
      case e: CantCompileQueryException => throw publicCannotCompile(e)
    }).get

    if (notifications.isEmpty) plan
    else ExecutionPlanWithNotifications(plan, notifications)
  }
}

case class CypherRuntimeConfiguration(workers: Int,
                                      morselSize: Int,
                                      doSchedulerTracing: Boolean,
                                      waitTimeout: Duration)

case class ExecutionPlanWithNotifications(inner: ExecutionPlan, extraNotifications: Set[InternalNotification]) extends DelegatingExecutionPlan(inner) {

  override def notifications: Set[InternalNotification] = inner.notifications ++ extraNotifications
}
