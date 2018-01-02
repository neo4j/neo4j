/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{EagerResultIterator, _}
import org.neo4j.cypher.internal.frontend.v3_4.phases.devNullLogger
import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.planner.v3_4.spi.IDPPlannerName
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.util.v3_4.Cardinality
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans.Argument
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

class ExecutionWorkflowBuilderTest extends CypherFunSuite {
  val PlannerName = IDPPlannerName
  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))
  val logicalPlan = {
    val x = Argument()(solved)
    x.assignIds()
    x
  }

  test("produces eager results for updating queries") {
    // GIVEN
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenReturn(Iterator.empty)
    val context = mock[QueryContext]
    when(context.transactionalContext).thenReturn(mock[QueryTransactionalContext])
    when(context.resources).thenReturn(mock[CloseableResource])

    val pipeInfo = PipeInfo(pipe, updating = true, None, None, PlannerName)
    val builderFactory = new InterpretedExecutionResultBuilderFactory(pipeInfo, List.empty, logicalPlan)

    // WHEN
    val builder = builderFactory.create()
    builder.setQueryContext(context)

    // THEN
    val result = builder.build("42", NormalMode, EMPTY_MAP, devNullLogger, InterpretedRuntimeName)
    result shouldBe a [PipeExecutionResult]
    result.asInstanceOf[PipeExecutionResult].result shouldBe a[EagerResultIterator]
  }

  test("produces lazy results for non-updating queries") {
    // GIVEN
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenReturn(Iterator.empty)
    val context = mock[QueryContext]
    val pipeInfo = PipeInfo(pipe, updating = false, None, None, PlannerName)
    val builderFactory = new InterpretedExecutionResultBuilderFactory(pipeInfo, List.empty, logicalPlan)

    // WHEN
    val builder = builderFactory.create()
    builder.setQueryContext(context)

    // THEN
    val result = builder.build("42", NormalMode, EMPTY_MAP, devNullLogger, InterpretedRuntimeName)
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
    val pipeInfo = PipeInfo(pipe, updating = false, None, None, PlannerName)
    val builderFactory = new InterpretedExecutionResultBuilderFactory(pipeInfo, List.empty, logicalPlan)

    // WHEN
    val builder = builderFactory.create()
    builder.setQueryContext(context)

    // THEN
    val result = builder.build("42", ExplainMode, EMPTY_MAP, devNullLogger, InterpretedRuntimeName)
    result shouldBe a [ExplainExecutionResult]
  }
}
