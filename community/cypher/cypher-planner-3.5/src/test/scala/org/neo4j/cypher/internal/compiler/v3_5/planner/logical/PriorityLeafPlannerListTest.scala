/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, verifyZeroInteractions, when}
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5.{QueryGraph, InterestingOrder}
import org.neo4j.cypher.internal.v3_5.logical.plans.Argument
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class PriorityLeafPlannerListTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  private val queryGraph = QueryGraph.empty
  private val candidates = Seq(Seq(Argument()))
  private val context = mock[LogicalPlanningContext]

  test("should use the priority list if that contains result") {
    // GIVEN
    val priority = mock[LeafPlannerIterable]
    val fallback = mock[LeafPlannerIterable]
    when(priority.candidates(any(), any(), any(), any())).thenReturn(candidates)
    val list = PriorityLeafPlannerList(priority, fallback)

    // WHEN
    val result = list.candidates(queryGraph, interestingOrder = InterestingOrder.empty, context = context)

    // THEN
    result should equal(candidates)
    verify(priority).candidates(any(), any(), any(), any())
    verifyZeroInteractions(fallback)
  }

  test("should use the fallback list if priority is empty") {
    // GIVEN
    val priority = mock[LeafPlannerIterable]
    val fallback = mock[LeafPlannerIterable]
    when(priority.candidates(any(), any(), any(), any())).thenReturn(Seq.empty)
    when(fallback.candidates(any(), any(), any(), any())).thenReturn(candidates)
    val list = PriorityLeafPlannerList(priority, fallback)

    // WHEN
    val result = list.candidates(queryGraph, interestingOrder = InterestingOrder.empty, context = context)

    // THEN
    result should equal(candidates)
    verify(priority).candidates(any(), any(), any(), any())
    verify(fallback).candidates(any(), any(), any(), any())
  }
}
