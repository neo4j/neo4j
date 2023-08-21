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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.GlobalMemory
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RenderSummaryTest extends CypherFunSuite {

  private val id = Id.INVALID_ID

  test("single node is represented nicely") {
    val arguments = Seq(
      Rows(42),
      DbHits(33)
    )

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set())

    renderSummary(plan) should equal("Total database accesses: 33")
  }

  test("single node no db hits") {
    val arguments = Seq(
      Rows(42),
      DbHits(0)
    )

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set())

    renderSummary(plan) should equal("Total database accesses: 0")
  }

  test("adds together two db hits") {
    val arguments1 = Seq(
      Rows(42),
      DbHits(33)
    )

    val arguments2 = Seq(
      Rows(42),
      DbHits(22)
    )

    val child = PlanDescriptionImpl(Id(0), "NAME1", NoChildren, arguments1, Set())
    val parent = PlanDescriptionImpl(Id(1), "NAME2", SingleChild(child), arguments2, Set())

    renderSummary(parent) should equal("Total database accesses: 55")
  }

  test("one node with db hits, one without") {
    val arguments1 = Seq(
      Rows(42),
      DbHits(33)
    )

    val arguments2 = Seq(
      Rows(42)
    )

    val child = PlanDescriptionImpl(Id(0), "NAME1", NoChildren, arguments1, Set())
    val parent = PlanDescriptionImpl(Id(1), "NAME2", SingleChild(child), arguments2, Set())

    renderSummary(parent) should equal("Total database accesses: 33 + ?")
  }

  test("execution plan without profiler stats uses question marks") {
    val arguments = Seq()

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set())

    renderSummary(plan) should equal("Total database accesses: ?")
  }

  test("should show total allocated memory") {
    val arguments = Seq(
      Rows(42),
      DbHits(33),
      GlobalMemory(1234L)
    )

    val plan = PlanDescriptionImpl(id, "NAME", NoChildren, arguments, Set())

    renderSummary(plan) should equal("Total database accesses: 33, total allocated memory: 1234")
  }
}
