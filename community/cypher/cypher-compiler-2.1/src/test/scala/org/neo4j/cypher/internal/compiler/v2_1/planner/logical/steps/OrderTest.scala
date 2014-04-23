/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{AscSortItem, Identifier, SignedIntegerLiteral}
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{Sort, Projection}

class OrderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should add sort if query graph contains sort items") {
    // given
    val sortItem = AscSortItem(Identifier("n")_)_
    val qg = QueryGraph(
      sortItems = Seq(sortItem)
    )
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )
    val input = newMockedLogicalPlan("n")

    // when
    val result = order(input)

    // then
    result should equal(Sort(input, Seq(sortItem)))
  }

  test("does not add sort when not needed") {
    // given
    val qg = QueryGraph()
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )
    val input = newMockedLogicalPlan("n")

    // when
    val result = order(input)

    // then
    result should equal(input)
  }
}
