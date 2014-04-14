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
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.PlanTable
import org.neo4j.cypher.{InternalException, SyntaxException}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{VarPatternLength, PatternRelationship, IdName}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.PatternRelationship
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.PlanTable

class ExtractBestPlanTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should throw when finding plan that does not solve all selections") {
    implicit val logicalPlanContext = newMockedLogicalPlanContext(
      planContext= newMockedPlanContext,
      queryGraph = MainQueryGraph(Map.empty, Selections(Seq(Set.empty[IdName] -> null)), Set(IdName("a"), IdName("b")), Set.empty, Seq.empty)
    )
    val plan = newMockedLogicalPlan("b")
    val planTable = PlanTable(Map(Set(IdName("a")) -> plan))

    evaluating {
      extractBestPlan(planTable)
    } should produce[CantHandleQueryException]
  }

  test("should throw when finding plan that does not solve all pattern relationships") {
    val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, VarPatternLength.unlimited)
    implicit val logicalPlanContext = newMockedLogicalPlanContext(
      planContext= newMockedPlanContext,
      queryGraph = MainQueryGraph(Map.empty, Selections(), Set(IdName("a"), IdName("b")), Set(patternRel), Seq.empty)
    )
    val plan = newMockedLogicalPlan("b")
    val planTable = PlanTable(Map(Set(IdName("a")) -> plan))

    evaluating {
      extractBestPlan(planTable)
    } should produce[CantHandleQueryException]
  }
}
