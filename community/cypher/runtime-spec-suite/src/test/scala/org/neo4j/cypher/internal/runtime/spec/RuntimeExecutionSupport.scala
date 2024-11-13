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
import org.neo4j.cypher.internal.runtime.QueryRuntimeConfig
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.TestPlanCombinationRewriterHint
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

/**
 * Methods to execute logical queries.
 *
 * Try to avoid overloaded method names
 */
trait RuntimeExecutionSupport[CONTEXT <: RuntimeContext] extends InputDataStreamTestSupport {

  /**
   * Compile a query
   * @return the execution plan
   */
  def buildPlan(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): ExecutionPlan

  /**
   * Compile a query
   * @return the execution plan and the used runtime context
   */
  def buildPlanAndContext(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): (ExecutionPlan, CONTEXT)

  /**
   * Execute a pre-compiled query with an ExecutionPlan
   */
  def executePlan(
    executablePlan: ExecutionPlan,
    readOnly: Boolean,
    implicitTx: Boolean,
    parameters: Map[String, Any],
    queryConfig: QueryRuntimeConfig
  ): RecordingRuntimeResult

  /**
   * Execute a LogicalQuery with InputDataStream.
   */
  def executeQuery(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream,
    parameters: Map[String, Any],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): RecordingRuntimeResult

  /**
   * Execute a LogicalQuery with a custom subscriber.
   */
  def executeWithSubscriber(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    subscriber: QuerySubscriber,
    inputStream: InputDataStream,
    parameters: Map[String, Any],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): RuntimeResult

  def executeAs(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    username: String,
    password: String,
    queryConfig: QueryRuntimeConfig
  ): RecordingRuntimeResult

  def executeWithoutValuePopulation(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream,
    parameters: Map[String, Any],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): RecordingRuntimeResult

  /**
   * Execute a logical query in its own transaction. Return the result already materialized.
   */
  def executeAndConsumeTransactionally(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig,
    profileAssertion: Option[QueryProfile => Unit],
    prePopulateResults: Boolean
  ): IndexedSeq[Array[AnyValue]]

  /**
   * Execute a logical query in its own transaction without recording the result. Return the non-recorded result.
   */
  def executeAndConsumeTransactionallyNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any] = Map.empty,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig,
    profileAssertion: Option[QueryProfile => Unit],
    prePopulateResults: Boolean
  ): Long

  /**
   * Execute a LogicalQuery with some input. Return the result and the context.
   */
  def executeAndContext(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues,
    parameters: Map[String, Any],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): (RecordingRuntimeResult, CONTEXT)

  /**
   * Execute a Logical query with some input without recording the results. Return the non-recording result and the context.
   */
  def executeAndContextNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues,
    parameters: Map[String, Any],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): (NonRecordingRuntimeResult, CONTEXT)

  /**
   * Execute a LogicalQuery with some input. Return the result and the execution plan description
   */
  def executeAndExplain(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues,
    queryConfig: QueryRuntimeConfig
  ): (RecordingRuntimeResult, InternalPlanDescription)

  /**
   * Profile a LogicalQuery with some input stream.
   */
  def profileQuery(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputDataStream: InputDataStream,
    parameters: Map[String, Any],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): RecordingRuntimeResult

  def profilePlan(
    executionPlan: ExecutionPlan,
    inputDataStream: InputDataStream,
    readOnly: Boolean,
    parameters: Map[String, Any],
    queryConfig: QueryRuntimeConfig
  ): RecordingRuntimeResult

  /**
   * Profile a LogicalQuery with some input stream without recording the result.
   */
  def profileNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputDataStream: InputDataStream,
    parameters: Map[String, Any],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): NonRecordingRuntimeResult

  def profileWithSubscriber(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    subscriber: QuerySubscriber,
    inputDataStream: InputDataStream,
    parameters: Map[String, Any],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): RuntimeResult

}
