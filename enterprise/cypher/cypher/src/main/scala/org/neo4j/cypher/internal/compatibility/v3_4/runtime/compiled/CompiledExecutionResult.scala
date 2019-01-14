/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
