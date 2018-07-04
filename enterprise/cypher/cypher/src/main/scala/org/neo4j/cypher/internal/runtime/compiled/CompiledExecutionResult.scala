/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.compiled

import java.util

import org.neo4j.cypher.internal.executionplan.GeneratedQueryExecution
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator

/**
  * Main class for compiled runtime results.
  */
class CompiledExecutionResult(context: QueryContext,
                              compiledCode: GeneratedQueryExecution,
                              override val queryProfile: QueryProfile)
  extends RuntimeResult {

  private var resultRequested = false

  def executionMode: ExecutionMode = compiledCode.executionMode()

  override def fieldNames(): Array[String] = compiledCode.fieldNames()

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    compiledCode.accept(visitor)
    resultRequested = true
  }

  override def queryStatistics() = QueryStatistics()

  override def isIterable: Boolean = false

  override def asIterator(): ResourceIterator[util.Map[String, AnyRef]] =
    throw new UnsupportedOperationException("The compiled runtime is not iterable")

  override def consumptionState: RuntimeResult.ConsumptionState =
    if (!resultRequested) ConsumptionState.NOT_STARTED
    else ConsumptionState.EXHAUSTED

  override def close(): Unit = {}
}
