/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.PatternExpression
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.graphdb.Direction

class ExtractBestPlanTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("should throw when finding plan that does not solve all selections") {
    val query = PlannerQuery(
      QueryGraph(
        selections = Selections(Set(Predicate(Set.empty[IdName], null))),
        patternNodes = Set(IdName("a"), IdName("b"))
      )
    )
    implicit val logicalPlanContext = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext)
    val plan = newMockedLogicalPlan("b")
    val planTable = greedyPlanTableWith(plan)

    evaluating {
      verifyBestPlan(planTable.uniquePlan, query)
    } should produce[CantHandleQueryException]
  }

  test("should throw when finding plan that does not solve all pattern relationships") {
    val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, VarPatternLength.unlimited)
    val query = PlannerQuery(
      QueryGraph(
        patternNodes = Set(IdName("a"), IdName("b")),
        patternRelationships = Set(patternRel)
      )
    )
    implicit val logicalPlanContext = newMockedLogicalPlanningContext(
      planContext= newMockedPlanContext
    )
    val plan = newMockedLogicalPlan("b")
    val planTable = greedyPlanTableWith(plan)

    evaluating {
      verifyBestPlan(planTable.uniquePlan, query)
    } should produce[CantHandleQueryException]
  }
}
