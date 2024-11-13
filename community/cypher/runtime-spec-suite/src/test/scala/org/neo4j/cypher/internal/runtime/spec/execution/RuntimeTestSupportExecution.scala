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
package org.neo4j.cypher.internal.runtime.spec.execution

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.QueryRuntimeConfig
import org.neo4j.cypher.internal.runtime.spec.NonRecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeExecutionSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.TestPlanCombinationRewriterHint
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

trait RuntimeTestSupportExecution[CONTEXT <: RuntimeContext] extends RuntimeExecutionSupport[CONTEXT] {

  protected def runtimeTestSupport: RuntimeTestSupport[CONTEXT]

  protected def defaultParameters: Map[String, Any] = Map.empty
  protected def defaultTestPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = Set.empty

  protected def defaultQueryRuntimeConfig: QueryRuntimeConfig =
    runtimeTestSupport.edition.defaultQueryRuntimeConfig
  protected def defaultReadOnly: Boolean = true
  protected def defaultImplicitTx: Boolean = false
  protected def defaultInputStream: InputDataStream = NoInput

  // Convenience overloaded methods
  def execute(executablePlan: ExecutionPlan): RecordingRuntimeResult = {
    executePlan(executablePlan, defaultReadOnly, defaultImplicitTx)
  }

  def execute(executablePlan: ExecutionPlan, readOnly: Boolean): RecordingRuntimeResult = {
    executePlan(executablePlan, readOnly, defaultImplicitTx)
  }

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT]
  ): RecordingRuntimeResult = {
    execute(logicalQuery, runtime, defaultInputStream)
  }

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any]
  ): RecordingRuntimeResult = {
    executeQuery(
      logicalQuery,
      runtime,
      defaultInputStream,
      parameters,
      defaultTestPlanCombinationRewriterHints,
      defaultQueryRuntimeConfig
    )
  }

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues
  ): RecordingRuntimeResult = {
    execute(logicalQuery, runtime, input.stream())
  }

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    subscriber: QuerySubscriber
  ): RuntimeResult = {
    executeWithSubscriber(logicalQuery, runtime, subscriber)
  }

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream
  ): RecordingRuntimeResult = {
    executeQuery(
      logicalQuery,
      runtime,
      inputStream,
      defaultParameters,
      defaultTestPlanCombinationRewriterHints,
      defaultQueryRuntimeConfig
    )
  }

  def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream,
    subscriber: QuerySubscriber
  ): RuntimeResult = {
    executeWithSubscriber(logicalQuery, runtime, subscriber, inputStream)
  }

  def profile(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT]
  ): RecordingRuntimeResult = {
    profile(logicalQuery, runtime, NoInput)
  }

  def profile(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues
  ): RecordingRuntimeResult = {
    profile(logicalQuery, runtime, input.stream())
  }

  def profile(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream
  ): RecordingRuntimeResult = {
    profileQuery(
      logicalQuery,
      runtime,
      inputStream,
      defaultParameters,
      defaultTestPlanCombinationRewriterHints,
      defaultQueryRuntimeConfig
    )
  }

  def profile(
    executionPlan: ExecutionPlan,
    inputStream: InputDataStream,
    readOnly: Boolean
  ): RecordingRuntimeResult = {
    profilePlan(executionPlan, inputStream, readOnly)
  }

  // RuntimeExecutionSupport methods

  override def buildPlan(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): ExecutionPlan = {
    runtimeTestSupport.buildPlan(logicalQuery, runtime, testPlanCombinationRewriterHints, queryConfig)
  }

  override def buildPlanAndContext(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): (ExecutionPlan, CONTEXT) = {
    runtimeTestSupport.buildPlanAndContext(logicalQuery, runtime, testPlanCombinationRewriterHints, queryConfig)
  }

  override def executePlan(
    executablePlan: ExecutionPlan,
    readOnly: Boolean,
    implicitTx: Boolean,
    parameters: Map[String, Any] = defaultParameters,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RecordingRuntimeResult = {
    runtimeTestSupport.executePlan(executablePlan, readOnly, implicitTx, parameters, queryConfig)
  }

  def executeWithInputValues(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RecordingRuntimeResult = {
    executeQuery(logicalQuery, runtime, input.stream(), parameters, testPlanCombinationRewriterHints, queryConfig)
  }

  override def executeQuery(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream = defaultInputStream,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RecordingRuntimeResult = {
    runtimeTestSupport.executeQuery(
      logicalQuery,
      runtime,
      inputStream,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig
    )
  }

  override def executeWithSubscriber(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    subscriber: QuerySubscriber,
    inputStream: InputDataStream = defaultInputStream,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RuntimeResult = {
    runtimeTestSupport.executeWithSubscriber(
      logicalQuery,
      runtime,
      subscriber,
      inputStream,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig
    )
  }

  override def executeAs(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    username: String,
    password: String,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RecordingRuntimeResult = {
    runtimeTestSupport.executeAs(logicalQuery, runtime, username, password, queryConfig)
  }

  override def executeWithoutValuePopulation(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RecordingRuntimeResult = {
    runtimeTestSupport.executeWithoutValuePopulation(
      logicalQuery,
      runtime,
      inputStream,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig
    )
  }

  override def executeAndConsumeTransactionally(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig,
    profileAssertion: Option[QueryProfile => Unit] = None,
    prePopulateResults: Boolean = true
  ): IndexedSeq[Array[AnyValue]] =
    runtimeTestSupport.executeAndConsumeTransactionally(
      logicalQuery,
      runtime,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig,
      profileAssertion,
      prePopulateResults
    )

  override def executeAndConsumeTransactionallyNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig,
    profileAssertion: Option[QueryProfile => Unit] = None,
    prePopulateResults: Boolean = true
  ): Long =
    runtimeTestSupport.executeAndConsumeTransactionallyNonRecording(
      logicalQuery,
      runtime,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig,
      profileAssertion,
      prePopulateResults
    )

  def profileWithInputValues(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RecordingRuntimeResult = {
    profileQuery(logicalQuery, runtime, input.stream(), parameters, testPlanCombinationRewriterHints, queryConfig)
  }

  override def profileQuery(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputDataStream: InputDataStream = defaultInputStream,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RecordingRuntimeResult = {
    runtimeTestSupport.profileQuery(
      logicalQuery.copy(doProfile = true),
      runtime,
      inputDataStream,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig
    )
  }

  override def profilePlan(
    executablePlan: ExecutionPlan,
    inputDataStream: InputDataStream,
    readOnly: Boolean,
    parameters: Map[String, Any] = defaultParameters,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RecordingRuntimeResult = {
    runtimeTestSupport.profilePlan(executablePlan, inputDataStream, readOnly, parameters, queryConfig)
  }

  override def profileNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputDataStream: InputDataStream = defaultInputStream,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): NonRecordingRuntimeResult = {
    runtimeTestSupport.profileNonRecording(
      logicalQuery.copy(doProfile = true),
      runtime,
      inputDataStream,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig
    )
  }

  override def profileWithSubscriber(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    subscriber: QuerySubscriber,
    inputDataStream: InputDataStream = defaultInputStream,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): RuntimeResult = {
    runtimeTestSupport.profileWithSubscriber(
      logicalQuery.copy(doProfile = true),
      runtime,
      subscriber,
      inputDataStream,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig
    )
  }

  override def executeAndContext(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): (RecordingRuntimeResult, CONTEXT) = {
    runtimeTestSupport.executeAndContext(
      logicalQuery,
      runtime,
      input,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig
    )
  }

  override def executeAndContextNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues,
    parameters: Map[String, Any] = defaultParameters,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = defaultTestPlanCombinationRewriterHints,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): (NonRecordingRuntimeResult, CONTEXT) = {
    runtimeTestSupport.executeAndContextNonRecording(
      logicalQuery,
      runtime,
      input,
      parameters,
      testPlanCombinationRewriterHints,
      queryConfig
    )
  }

  override def executeAndExplain(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues,
    queryConfig: QueryRuntimeConfig = defaultQueryRuntimeConfig
  ): (RecordingRuntimeResult, InternalPlanDescription) = {
    runtimeTestSupport.executeAndExplain(logicalQuery, runtime, input, queryConfig)
  }
}
