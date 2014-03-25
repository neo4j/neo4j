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
import org.neo4j.cypher.internal.compiler.v2_1.planner.{LogicalPlanningTestSupport, Selections, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, SignedIntegerLiteral}
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection

class ProjectTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should add projection for expressions not already covered") {
    // given
    val projections = Map("42" -> SignedIntegerLiteral("42")(DummyPosition(0)))
    val qg = QueryGraph(projections, Selections(), patternNodes = Set.empty, patternRelationships = Set.empty)
    implicit val context = createContextWith(qg)
    val input = newMockedLogicalPlan("n")

    // when
    val result = project(input)

    // then
    result should equal(Projection(input, projections))
  }

  test("does not add projection when not needed") {
    // given
    val projections = Map("n" -> Identifier("n")(DummyPosition(0)))
    val qg = QueryGraph(projections, Selections(), patternNodes = Set.empty, patternRelationships = Set.empty)
    implicit val context = createContextWith(qg)
    val input = newMockedLogicalPlan("n")

    // when
    val result = project(input)

    // then
    result should equal(input)
  }

  private def createContextWith(qg:QueryGraph):LogicalPlanContext = newMockedLogicalPlanContext(queryGraph = qg)
}
