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
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanConstructionTestSupport, PlannerQuery, QueryGraph}
import org.neo4j.graphdb.Direction

class ExpandTableSolverTest extends CypherFunSuite with LogicalPlanConstructionTestSupport {

  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._

  val solvable1 = mock[Solvable]

  val plan1 = mock[LogicalPlan]
  val plan2 = mock[LogicalPlan]
  when(plan1.solved).thenReturn(PlannerQuery.empty)
  when(plan2.solved).thenReturn(PlannerQuery.empty)

  val table = new ExhaustivePlanTable

  val qg = mock[QueryGraph]

  test("does not expand based on empty table") {
    val solvable2 = mock[Solvable]

    expandTableSolver(qg, Set(solvable1, solvable2), table) should be(empty)
  }

  test("expands if an unsolved pattern relationship overlaps once with a single solved plan") {
    val pattern = PatternRelationship('r2, ('b, 'c), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val solvable2 = SolvableRelationship(pattern)

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))

    table.put(Set(solvable1), plan1)

    expandTableSolver(qg, Set(solvable1, solvable2), table).toSet should equal(Set(
      planSimpleExpand(plan1, 'b, Direction.OUTGOING, 'c, pattern, ExpandAll)
    ))
  }

  test("expands if an unsolved pattern relationships overlaps twice with a single solved plan") {
    val pattern = PatternRelationship('r2, ('a, 'b), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val solvable2 = SolvableRelationship(pattern)

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))

    table.put(Set(solvable1), plan1)

    expandTableSolver(qg, Set(solvable1, solvable2), table).toSet should equal(Set(
      planSimpleExpand(plan1, 'a, Direction.OUTGOING, 'b, pattern, ExpandInto),
      planSimpleExpand(plan1, 'b, Direction.INCOMING, 'a, pattern, ExpandInto)
    ))
  }

  test("does not expand if an unsolved pattern relationship does not overlap with a solved plan") {
    val pattern = PatternRelationship('r2, ('x, 'y), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val solvable2 = SolvableRelationship(pattern)

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))

    table.put(Set(solvable1), plan1)

    expandTableSolver(qg, Set(solvable1, solvable2), table).toSet should be(empty)
  }

  test("expands if an unsolved pattern relationship overlaps with multiple solved plans") {
    val solvable2 = mock[Solvable]
    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b, 'c, 'r2, 'd))

    table.put(Set(solvable1, solvable2), plan1)

    val pattern = PatternRelationship('r3, ('b, 'c), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val solvable3 = SolvableRelationship(pattern)

    expandTableSolver(qg, Set(solvable1, solvable2, solvable3), table).toSet should equal(Set(
      planSimpleExpand(plan1, 'b, Direction.OUTGOING, 'c, pattern, ExpandInto),
      planSimpleExpand(plan1, 'c, Direction.INCOMING, 'b, pattern, ExpandInto)
    ))
  }
}
