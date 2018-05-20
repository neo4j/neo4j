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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{EagerResultIterator, _}
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanConstructionTestSupport
import org.opencypher.v9_0.frontend.phases.{InternalNotificationLogger, devNullLogger}
import org.neo4j.cypher.internal.planner.v3_5.spi.IDPPlannerName
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.logical.plans.Argument
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

class ExecutionWorkflowBuilderTest extends CypherFunSuite with LogicalPlanConstructionTestSupport {

  private val PlannerName = IDPPlannerName
  private val logicalPlan = Argument()

  test("produces eager results for updating queries") {
    // GIVEN
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenReturn(Iterator.empty)
    val context = mock[QueryContext]
    when(context.transactionalContext).thenReturn(mock[QueryTransactionalContext])
    when(context.resources).thenReturn(mock[CloseableResource])

    val builderFactory = InterpretedExecutionResultBuilderFactory(pipe, readOnly = false, List.empty, logicalPlan)

    // WHEN
    val builder = builderFactory.create()
    builder.setQueryContext(context)

    // THEN
    val result = build(builder, NormalMode, EMPTY_MAP, devNullLogger, InterpretedRuntimeName, readOnly = false)
    result shouldBe a [PipeExecutionResult]
    result.asInstanceOf[PipeExecutionResult].result shouldBe a[EagerResultIterator]
  }

  test("produces lazy results for non-updating queries") {
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenReturn(Iterator.empty)
    val context = mock[QueryContext]
    val builderFactory = new InterpretedExecutionResultBuilderFactory(pipe, readOnly = true, List.empty, logicalPlan)

    // WHEN
    val builder = builderFactory.create()
    builder.setQueryContext(context)

    // THEN
    val result = build(builder, NormalMode, EMPTY_MAP, devNullLogger, InterpretedRuntimeName, readOnly = true)
    result shouldBe a [PipeExecutionResult]
    result.asInstanceOf[PipeExecutionResult].result should not be an[EagerResultIterator]
  }

  test("produces explain results for EXPLAIN queries") {
    // GIVEN
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenReturn(Iterator.empty)
    val context = mock[QueryContext]
    when(context.transactionalContext).thenReturn(mock[QueryTransactionalContext])
    when(context.resources).thenReturn(mock[CloseableResource])
    val builderFactory = InterpretedExecutionResultBuilderFactory(pipe, readOnly = true, List.empty, logicalPlan)

    // WHEN
    val builder = builderFactory.create()
    builder.setQueryContext(context)

    // THEN
    val result = build(builder, ExplainMode, EMPTY_MAP, devNullLogger, InterpretedRuntimeName, readOnly = true)
    result shouldBe a [ExplainExecutionResult]
  }

  private def build(builder: ExecutionResultBuilder,
                    planType: ExecutionMode,
                    params: MapValue,
                    notificationLogger: InternalNotificationLogger,
                    runtimeName: RuntimeName,
                    readOnly: Boolean) =
    builder.build(planType, params, notificationLogger, PlannerName, runtimeName, readOnly, new StubCardinalities)
}
