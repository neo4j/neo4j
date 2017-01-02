/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_1.executionplan

import java.util

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_1.spi.{InternalResultVisitor, QueryContext}
import org.neo4j.cypher.internal.frontend.v3_1.ProfilerStatisticsNotReadyException

trait SuccessfulCloseable {
  def success(): Unit
  def close(): Unit
}

/**
 * Main class for compiled execution results, implements everything in InternalExecutionResult
 * except `javaColumns` and `accept` which delegates to the injected compiled code.
 */
class CompiledExecutionResult(taskCloser: TaskCloser,
                              context: QueryContext,
                              compiledCode: GeneratedQueryExecution,
                              description: Provider[InternalPlanDescription])
  extends StandardInternalExecutionResult(context, Some(taskCloser))
  with StandardInternalExecutionResult.IterateByAccepting {

  compiledCode.setSuccessfulCloseable(this)

  // *** Delegate to compiled code
  def executionMode: ExecutionMode = compiledCode.executionMode()

  override def javaColumns: util.List[String] = compiledCode.javaColumns()

  override def accept[EX <: Exception](visitor: InternalResultVisitor[EX]): Unit =
    compiledCode.accept(visitor)

  override def executionPlanDescription(): InternalPlanDescription = {
    if (!taskCloser.isClosed) throw new ProfilerStatisticsNotReadyException

    compiledCode.executionPlanDescription()
  }

  override def queryStatistics() = InternalQueryStatistics()

  //TODO delegate to compiled code once writes are being implemented
  override def executionType = READ_ONLY
}
