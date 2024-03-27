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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.InputDataStreamTestSupport
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.TestPlanCombinationRewriterHint
import org.neo4j.cypher.result.QueryProfile
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
  def buildPlan(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = Set.empty[TestPlanCombinationRewriterHint]
  ): ExecutionPlan

  /**
   * Compile a query
   * @return the execution plan and the used runtime context
   */
  def buildPlanAndContext(logicalQuery: LogicalQuery, runtime: CypherRuntime[CONTEXT]): (ExecutionPlan, CONTEXT)

  /**
   * Execute an pre-compiled query with an ExecutionPlan
   */
  def execute(executablePlan: ExecutionPlan): RecordingRuntimeResult =
    execute(executablePlan, readOnly = true, implicitTx = false)

  /**
   * Execute an pre-compiled query with an ExecutionPlan
   */
  def execute(executablePlan: ExecutionPlan, readOnly: Boolean): RecordingRuntimeResult =
    execute(executablePlan, readOnly, implicitTx = false)

  /**
   * Execute an pre-compiled query with an ExecutionPlan
   */
  def execute(executablePlan: ExecutionPlan, readOnly: Boolean, implicitTx: Boolean): RecordingRuntimeResult

  /**
   * Execute a Logical query.
   */
  def execute(logicalQuery: LogicalQuery, runtime: CypherRuntime[CONTEXT]): RecordingRuntimeResult =
    execute(logicalQuery, runtime, NoInput, Map.empty[String, Any])

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
  ): RecordingRuntimeResult

  def executeAs(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    username: String,
    password: String
  ): RecordingRuntimeResult

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any]
  ): RecordingRuntimeResult =
    execute(logicalQuery, runtime, NoInput, parameters)

  /**
   * Execute a Logical query with some input.
   */
  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues
  ): RecordingRuntimeResult = {
    execute(logicalQuery, runtime, input.stream(), Map.empty[String, Any])
  }

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
  ): RecordingRuntimeResult

  /**
   * Execute a Logical query with some input stream.
   */
  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream
  ): RecordingRuntimeResult = execute(logicalQuery, runtime, inputStream, Map.empty[String, Any])

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream,
    parameters: Map[String, Any]
  ): RecordingRuntimeResult

  def executeWithoutValuePopulation(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream,
    parameters: Map[String, Any]
  ): RecordingRuntimeResult

  /**
   * Execute a Logical query with a custom subscriber.
   */
  def execute(logicalQuery: LogicalQuery, runtime: CypherRuntime[CONTEXT], subscriber: QuerySubscriber): RuntimeResult =
    execute(logicalQuery, runtime, NoInput, subscriber)

  /**
   * Execute a Logical query with a custom subscriber and some input.
   */
  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputDataStream,
    subscriber: QuerySubscriber
  ): RuntimeResult = {
    execute(logicalQuery, runtime, input, subscriber, Set.empty[TestPlanCombinationRewriterHint])
  }

  /**
   * Execute a Logical query with a custom subscriber and some input and test plan combination rewriter hints.
   */
  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputDataStream,
    subscriber: QuerySubscriber,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
  ): RuntimeResult

  /**
   * Execute a logical query in its own transaction. Return the result already materialized.
   */
  def executeAndConsumeTransactionally(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any] = Map.empty,
    profileAssertion: Option[QueryProfile => Unit] = None,
    prePopulateResults: Boolean = true
  ): IndexedSeq[Array[AnyValue]]

  /**
   * Execute a logical query in its own transaction without recording the result. Return the non-recorded result.
   */
  def executeAndConsumeTransactionallyNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any] = Map.empty,
    profileAssertion: Option[QueryProfile => Unit] = None,
    prePopulateResults: Boolean = true
  ): Long

  /**
   * Execute a Logical query with some input. Return the result and the context.
   */
  def executeAndContext(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues
  ): (RecordingRuntimeResult, CONTEXT)

  /**
   * Execute a Logical query with some input without recording the results. Return the non-recording result and the context.
   */
  def executeAndContextNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues
  ): (NonRecordingRuntimeResult, CONTEXT)

  /**
   * Execute a Logical query with some input. Return the result and the execution plan description
   */
  def executeAndExplain(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues
  ): (RecordingRuntimeResult, InternalPlanDescription)

  /**
   * Profile a Logical query with some input.
   */
  def profile(logicalQuery: LogicalQuery, runtime: CypherRuntime[CONTEXT], input: InputValues): RecordingRuntimeResult =
    profile(logicalQuery, runtime, input.stream())

  /**
   * Profile a Logical query with some input stream.
   */
  def profile(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputDataStream: InputDataStream = NoInput,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = Set.empty[TestPlanCombinationRewriterHint]
  ): RecordingRuntimeResult

  def profile(executionPlan: ExecutionPlan, inputDataStream: InputDataStream, readOnly: Boolean): RecordingRuntimeResult

  /**
   * Profile a Logical query with some input stream without recording the result.
   */
  def profileNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputDataStream: InputDataStream = NoInput
  ): NonRecordingRuntimeResult

  def profileWithSubscriber(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    subscriber: QuerySubscriber,
    inputDataStream: InputDataStream = NoInput
  ): RuntimeResult

}
