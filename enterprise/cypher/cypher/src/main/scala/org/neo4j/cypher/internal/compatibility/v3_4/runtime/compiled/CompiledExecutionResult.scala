/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled

import org.neo4j.cypher.internal.util.v3_4.{ProfilerStatisticsNotReadyException, TaskCloser}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{Provider, StandardInternalExecutionResult}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.CompiledRuntimeName
import org.neo4j.cypher.internal.v3_4.executionplan.GeneratedQueryExecution
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb.Notification

/**
  * Main class for compiled execution results, implements everything in InternalExecutionResult
  * except `javaColumns` and `accept` which delegates to the injected compiled code.
  */
class CompiledExecutionResult(taskCloser: TaskCloser,
                              context: QueryContext,
                              compiledCode: GeneratedQueryExecution,
                              description: Provider[InternalPlanDescription],
                              notifications: Iterable[Notification] = Iterable.empty)
  extends StandardInternalExecutionResult(context, CompiledRuntimeName, Some(taskCloser))
    with StandardInternalExecutionResult.IterateByAccepting {

  compiledCode.setCompletable(this)

  // *** Delegate to compiled code
  def executionMode: ExecutionMode = compiledCode.executionMode()

  override def fieldNames(): Array[String] = compiledCode.fieldNames()

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit =
    compiledCode.accept(visitor)

  override def executionPlanDescription(): InternalPlanDescription = {
    if (!taskCloser.isClosed && executionMode == ProfileMode) {
      completed(success = false)
      throw new ProfilerStatisticsNotReadyException
    }

    compiledCode.executionPlanDescription()
      .addArgument(Runtime(CompiledRuntimeName.toTextOutput))
      .addArgument(RuntimeImpl(CompiledRuntimeName.name))
  }

  override def queryStatistics() = QueryStatistics()

  //TODO delegate to compiled code once writes are being implemented
  override def queryType: InternalQueryType = READ_ONLY

  override def withNotifications(notification: Notification*): InternalExecutionResult =
    new CompiledExecutionResult(taskCloser, context, compiledCode, description, notification)
}
