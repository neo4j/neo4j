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
package org.neo4j.cypher.internal.compatibility.v3_2

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{SchemaIndexSeekUsage, ExecutionPlan => ExecutionPlan_v3_2}
import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_2.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_3.{TransactionalContextWrapper => TransactionalContextWrapperV3_3}
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.impl.query.QueryExecutionMonitor

class CompatibilityTest extends CypherFunSuite {

  test("make sure that ExecutionPlanWrapper uses TC at creation time") {
    val searchMonitor = mock[IndexSearchMonitor]
    val executionMonitor = mock[QueryExecutionMonitor]
    val executionPlan = mock[ExecutionPlan_v3_2](withSettings.defaultAnswer(RETURNS_DEEP_STUBS))
    when(executionPlan.runtimeUsed.name).thenReturn("Satia's favorite runtime")
    when(executionPlan.plannerUsed.name).thenReturn("Satia's favorite planner")
    when(executionPlan.plannedIndexUsage).thenReturn(Seq(SchemaIndexSeekUsage("id", "label", Seq())))

    var txClosed = false

    val contextWrapper = mock[TransactionalContextWrapperV3_3]
    when(contextWrapper.readOperations).thenAnswer(new Answer[ReadOperations] {
      override def answer(invocationOnMock: InvocationOnMock): ReadOperations = {
        if(txClosed)
          null
        else
          mock[ReadOperations]
      }
    })

    // When
    val executionPlanWrapper = new ExecutionPlanWrapper(executionPlan, contextWrapper, Set(), InputPosition.NONE,
      searchMonitor, executionMonitor)
    // and when
    txClosed = true

    // then does not fail
    executionPlanWrapper.plannerInfo
  }

}
