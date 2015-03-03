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
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanConstructionTestSupport, PlannerQuery, QueryGraph}

class JoinTableSolverTest extends CypherFunSuite with LogicalPlanConstructionTestSupport {

  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._

  val solvable1 = mock[Solvable]
  val solvable2 = mock[Solvable]

  val plan1 = mock[LogicalPlan]
  val plan2 = mock[LogicalPlan]
  when(plan1.solved).thenReturn(PlannerQuery.empty)
  when(plan2.solved).thenReturn(PlannerQuery.empty)

  val table = new ExhaustivePlanTable

  val qg = mock[QueryGraph]
  when(qg.argumentIds).thenReturn(Set.empty[IdName])

  test("does not join based on empty table") {
    joinTableSolver(qg, Set(solvable1, solvable2), table) should be(empty)
  }

  test("joins plans that solve a single pattern relationship") {
    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))
    when(plan1.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('a, 'b)))
    when(plan2.availableSymbols).thenReturn(Set[IdName]('b, 'r2, 'c))
    when(plan2.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('b, 'c)))

    table.put(Set(solvable1), plan1)
    table.put(Set(solvable2), plan2)

    joinTableSolver(qg, Set(solvable1, solvable2), table).toSet should equal(Set(
      planNodeHashJoin(Set('b), plan1, plan2),
      planNodeHashJoin(Set('b), plan2, plan1)
    ))
  }

  test("does not join plans that do not overlap") {
    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))
    when(plan1.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('a, 'b)))
    when(plan2.availableSymbols).thenReturn(Set[IdName]('c, 'r2, 'd))
    when(plan2.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('c, 'd)))

    table.put(Set(solvable1), plan1)
    table.put(Set(solvable2), plan2)

    joinTableSolver(qg, Set(solvable1, solvable2), table) should be(empty)
  }

  test("does not join plans that overlap on non-nodes") {
    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b, 'x))
    when(plan1.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('a, 'b)))
    when(plan2.availableSymbols).thenReturn(Set[IdName]('c, 'r2, 'd, 'x))
    when(plan2.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('c, 'd)))

    table.put(Set(solvable1), plan1)
    table.put(Set(solvable2), plan2)

    joinTableSolver(qg, Set(solvable1, solvable2), table) should be(empty)
  }

  test("does not join plans that overlap on nodes that are arguments") {
    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b, 'x))
    when(plan1.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('a, 'b, 'x)))
    when(plan2.availableSymbols).thenReturn(Set[IdName]('c, 'r2, 'd, 'x))
    when(plan2.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('c, 'd, 'x)))
    when(qg.argumentIds).thenReturn(Set[IdName]('x))

    table.put(Set(solvable1), plan1)
    table.put(Set(solvable2), plan2)

    joinTableSolver(qg, Set(solvable1, solvable2), table) should be(empty)
  }
}
