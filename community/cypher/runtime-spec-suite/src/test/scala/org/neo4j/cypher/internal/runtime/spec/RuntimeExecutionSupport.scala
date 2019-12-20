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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.runtime.{InputDataStream, InputDataStreamTestSupport, InputValues, NoInput}
import org.neo4j.cypher.internal.{CypherRuntime, ExecutionPlan, LogicalQuery, RuntimeContext}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

/**
 * Methods to execute logical queries.
 */
trait RuntimeExecutionSupport[CONTEXT <: RuntimeContext] extends InputDataStreamTestSupport {

  /**
   * Compile a query
   * @return the execution plan
   */
  def buildPlan(logicalQuery: LogicalQuery,
                runtime: CypherRuntime[CONTEXT]): ExecutionPlan

  /**
   * Execute an pre-compiled query with an ExecutionPlan
   */
  def execute(executablePlan: ExecutionPlan): RecordingRuntimeResult

  /**
   * Execute a Logical query.
   */
  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT]
             ): RecordingRuntimeResult = execute(logicalQuery, runtime, NoInput)

  /**
   * Execute a Logical query with some input.
   */
  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              input: InputValues): RecordingRuntimeResult = execute(logicalQuery, runtime, input.stream())

  /**
   * Execute a Logical query with some input stream.
   */
  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              inputStream: InputDataStream): RecordingRuntimeResult

  /**
   * Execute a Logical query with a custom subscriber.
   */
  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              subscriber: QuerySubscriber): RuntimeResult = execute(logicalQuery, runtime, NoInput, subscriber)

  /**
   * Execute a Logical query with a custom subscriber and some input.
   */
  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              input: InputDataStream,
              subscriber: QuerySubscriber): RuntimeResult

  /**
   * Execute a logical query in its own transaction. Return the result already materialized.
   */
  def executeAndConsumeTransactionally(logicalQuery: LogicalQuery,
                                       runtime: CypherRuntime[CONTEXT],
                                       parameters: Map[String, Any] = Map.empty
                                      ): IndexedSeq[Array[AnyValue]]

  /**
   * Execute a Logical query with some input. Return the result andthe context.
   */
  def executeAndContext(logicalQuery: LogicalQuery,
                        runtime: CypherRuntime[CONTEXT],
                        input: InputValues
                       ): (RecordingRuntimeResult, CONTEXT)

  /**
   * Profile a Logical query with some input.
   */
  def profile(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              input: InputValues): RecordingRuntimeResult = profile(logicalQuery, runtime, input.stream())
  /**
   * Profile a Logical query with some input stream.
   */
  def profile(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              inputDataStream: InputDataStream = NoInput): RecordingRuntimeResult
}
