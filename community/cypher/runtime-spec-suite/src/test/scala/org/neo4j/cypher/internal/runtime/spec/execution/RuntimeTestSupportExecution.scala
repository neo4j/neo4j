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

  override def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputDataStream,
    subscriber: QuerySubscriber,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
  ): RuntimeResult =
    runtimeTestSupport.execute(logicalQuery, runtime, input, subscriber, testPlanCombinationRewriterHints)

  override def execute(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputStream: InputDataStream,
    parameters: Map[String, Any]
  ): RecordingRuntimeResult = runtimeTestSupport.execute(logicalQuery, runtime, inputStream, parameters)

  override def executeAndConsumeTransactionally(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any] = Map.empty,
    profileAssertion: Option[QueryProfile => Unit] = None
  ): IndexedSeq[Array[AnyValue]] =
    runtimeTestSupport.executeAndConsumeTransactionally(logicalQuery, runtime, parameters, profileAssertion)

  override def executeAndConsumeTransactionallyNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any] = Map.empty,
    profileAssertion: Option[QueryProfile => Unit] = None
  ): Long =
    runtimeTestSupport.executeAndConsumeTransactionallyNonRecording(logicalQuery, runtime, parameters, profileAssertion)

  override def execute(executablePlan: ExecutionPlan, readOnly: Boolean, implicitTx: Boolean): RecordingRuntimeResult =
    runtimeTestSupport.execute(executablePlan, readOnly, implicitTx)

  override def buildPlan(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
  ): ExecutionPlan =
    runtimeTestSupport.buildPlan(logicalQuery, runtime)

  override def buildPlanAndContext(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT]
  ): (ExecutionPlan, CONTEXT) = runtimeTestSupport.buildPlanAndContext(logicalQuery, runtime)

  override def profile(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputDataStream: InputDataStream = NoInput,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = Set.empty[TestPlanCombinationRewriterHint]
  ): RecordingRuntimeResult = runtimeTestSupport.profile(
    logicalQuery.copy(doProfile = true),
    runtime,
    inputDataStream,
    testPlanCombinationRewriterHints
  )

  override def profile(
    executablePlan: ExecutionPlan,
    inputDataStream: InputDataStream,
    readOnly: Boolean
  ): RecordingRuntimeResult = runtimeTestSupport.profile(executablePlan, inputDataStream, readOnly)

  override def profileNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    inputDataStream: InputDataStream = NoInput
  ): NonRecordingRuntimeResult =
    runtimeTestSupport.profileNonRecording(logicalQuery.copy(doProfile = true), runtime, inputDataStream)

  override def profileWithSubscriber(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    subscriber: QuerySubscriber,
    inputDataStream: InputDataStream = NoInput
  ): RuntimeResult =
    runtimeTestSupport.profileWithSubscriber(logicalQuery, runtime, subscriber, inputDataStream)

  override def executeAndContext(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues
  ): (RecordingRuntimeResult, CONTEXT) = runtimeTestSupport.executeAndContext(logicalQuery, runtime, input)

  override def executeAndContextNonRecording(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues
  ): (NonRecordingRuntimeResult, CONTEXT) =
    runtimeTestSupport.executeAndContextNonRecording(logicalQuery, runtime, input)

  override def executeAndExplain(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    input: InputValues
  ): (RecordingRuntimeResult, InternalPlanDescription) =
    runtimeTestSupport.executeAndExplain(logicalQuery, runtime, input)

}
