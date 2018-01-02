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
package org.neo4j.cypher.internal.compiler.v2_3.planDescription

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{SingleRowPipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class RenderSummaryTest extends CypherFunSuite {

  val pipe = SingleRowPipe()(mock[PipeMonitor])

  test("single node is represented nicely") {
    val arguments = Seq(
      Rows(42),
      DbHits(33))

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set())

    renderSummary(plan) should equal("Total database accesses: 33")
  }

  test("adds together two db hits") {
    val arguments1 = Seq(
      Rows(42),
      DbHits(33))

    val arguments2 = Seq(
      Rows(42),
      DbHits(22))

    val child = PlanDescriptionImpl(new Id, "NAME1", NoChildren, arguments1, Set())
    val parent = PlanDescriptionImpl(new Id, "NAME2", SingleChild(child), arguments2, Set())

    renderSummary(parent) should equal("Total database accesses: 55")  }

  test("execution plan without profiler stats uses question marks") {
    val arguments = Seq()

    val plan = PlanDescriptionImpl(new Id, "NAME", NoChildren, arguments, Set())

    renderSummary(plan) should equal("Total database accesses: ?")  }
}
