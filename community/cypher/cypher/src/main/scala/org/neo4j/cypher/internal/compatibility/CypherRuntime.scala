/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher.{CypherRuntimeOption, InvalidArgumentException, exceptionHandler}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_5.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException

import scala.util.{Failure, Try}

/**
  * A cypher runtime. Compiles logical plans into a executable form, which can
  * be used directly to serve the query.
  */
trait CypherRuntime[-CONTEXT <: CommunityRuntimeContext] {

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
  * Cypher runtime representing a user-selected runtime which is not supported.
  */
object UnknownRuntime extends CypherRuntime[CommunityRuntimeContext] {
  def compileToExecutable(logicalPlan: LogicalPlanState, context: CommunityRuntimeContext): ExecutionPlan =
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
class FallbackRuntime[CONTEXT <: CommunityRuntimeContext](runtimes: Seq[CypherRuntime[CONTEXT]],
                                                          requestedRuntime: CypherRuntimeOption) extends CypherRuntime[CONTEXT] {

  private val PublicCannotCompile =
    {
      val message = s"This version of Neo4j does not support requested runtime: ${requestedRuntime.name}"
      val invalidArgument = new InvalidArgumentException(message)
      new org.neo4j.graphdb.QueryExecutionException(message, invalidArgument, invalidArgument.status.code().serialize())
    }

  override def compileToExecutable(logicalPlan: LogicalPlanState, context: CONTEXT): ExecutionPlan = {
    var executionPlan: Try[ExecutionPlan] = Try(ProcedureCallOrSchemaCommandRuntime.compileToExecutable(logicalPlan, context))

    for (runtime <- runtimes if executionPlan.isFailure) {
      executionPlan =
        Try(
          exceptionHandler.runSafely(
            runtime.compileToExecutable(logicalPlan, context)
          )
        )

      if (executionPlan.isFailure && requestedRuntime != CypherRuntimeOption.default)
        context.notificationLogger.log(RuntimeUnsupportedNotification)
    }

    executionPlan.recover({
      case e: CantCompileQueryException => throw PublicCannotCompile
    }).get
  }
}
