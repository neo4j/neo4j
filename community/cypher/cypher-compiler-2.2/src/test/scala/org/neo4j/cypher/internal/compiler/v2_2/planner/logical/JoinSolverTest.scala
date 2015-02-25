/**
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.PlannerQuery
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, LogicalPlan, PatternRelationship}

class JoinSolverTest extends CypherFunSuite {

  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._

  val solvable1 = mock[Solvable]
  val solvable2 = mock[Solvable]

  val plan1 = mock[LogicalPlan]
  val plan2 = mock[LogicalPlan]
  when(plan1.solved).thenReturn(PlannerQuery.empty)
  when(plan2.solved).thenReturn(PlannerQuery.empty)

  val table = new ExhaustivePlanTable

  test("does not join base don empty table") {
    joinTableSolver(Set(solvable1, solvable2), table) should be(empty)
  }

  test("joins plans that solve a single pattern relationship") {
    when(plan1.availableSymbols).thenReturn(ids('a, 'r1, 'b))
    when(plan2.availableSymbols).thenReturn(ids('b, 'r2, 'c))

    table.put(Set(solvable1), plan1)
    table.put(Set(solvable2), plan2)

    joinTableSolver(Set(solvable1, solvable2), table).toSet should equal(Set(
      planNodeHashJoin(ids('b), plan1, plan2),
      planNodeHashJoin(ids('b), plan2, plan1)
    ))
  }

  test("does not join plans that do not overlap") {
    when(plan1.availableSymbols).thenReturn(ids('a, 'r1, 'b))
    when(plan2.availableSymbols).thenReturn(ids('c, 'r2, 'd))

    table.put(Set(solvable1), plan1)
    table.put(Set(solvable2), plan2)

    joinTableSolver(Set(solvable1, solvable2), table) should be(empty)
  }

  private def ids(ids: Symbol*) = ids.map(_.toString()).map(IdName.apply).toSet
}
