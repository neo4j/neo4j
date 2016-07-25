/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical

import org.neo4j.cypher.internal.compiler.v3_1.planner.{QueryGraph, LogicalPlanningTestSupport2}
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.{LogicalPlan, SingleRow}
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyZeroInteractions
import org.mockito.Mockito.when
import org.mockito.Matchers.any

class PriorityLeafPlannerListTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  private val queryGraph = QueryGraph.empty
  private val candidates = Seq(Seq(SingleRow()(solved)))
  private implicit val context = mock[LogicalPlanningContext]

  test("should use the priority list if that contains result") {
    // GIVEN
    val priority = mock[LeafPlannerIterable]
    val fallback = mock[LeafPlannerIterable]
    when(priority.candidates(any(), any())(any())).thenReturn(candidates)
    val list = PriorityLeafPlannerList(priority, fallback)

    // WHEN
    val result = list.candidates(queryGraph)

    // THEN
    result should equal(candidates)
    verify(priority).candidates(any(), any())(any())
    verifyZeroInteractions(fallback)
  }

  test("should use the fallback list if priority is empty") {
    // GIVEN
    val priority = mock[LeafPlannerIterable]
    val fallback = mock[LeafPlannerIterable]
    when(priority.candidates(any(), any())(any())).thenReturn(Seq.empty)
    when(fallback.candidates(any(), any())(any())).thenReturn(candidates)
    val list = PriorityLeafPlannerList(priority, fallback)

    // WHEN
    val result = list.candidates(queryGraph)

    // THEN
    result should equal(candidates)
    verify(priority).candidates(any(), any())(any())
    verify(fallback).candidates(any(), any())(any())
  }
}
