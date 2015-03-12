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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanConstructionTestSupport, PlannerQuery, QueryGraph}
import org.neo4j.graphdb.Direction

class ExpandSolverStepTest extends CypherFunSuite with LogicalPlanConstructionTestSupport {

  import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.LogicalPlanProducer._

  val plan1 = mock[LogicalPlan]
  val plan2 = mock[LogicalPlan]
  when(plan1.solved).thenReturn(PlannerQuery.empty)
  when(plan2.solved).thenReturn(PlannerQuery.empty)

  val pattern1 = PatternRelationship('r1, ('a, 'b), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val pattern2 = PatternRelationship('r2, ('b, 'c), Direction.OUTGOING, Seq.empty, SimplePatternLength)

  val table = new IDPTable[LogicalPlan]()
  val qg = mock[QueryGraph]

  test("does not expand based on empty table") {
    implicit val registry = IdRegistry[PatternRelationship]

    expandSolverStep(qg)(registry, register(pattern1, pattern2), table) should be(empty)
  }

  test("expands if an unsolved pattern relationship overlaps once with a single solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))
    table.put(register(pattern1), plan1)

    expandSolverStep(qg)(registry, register(pattern1, pattern2), table).toSet should equal(Set(
      planSimpleExpand(plan1, 'b, Direction.OUTGOING, 'c, pattern2, ExpandAll)
    ))
  }

  test("expands if an unsolved pattern relationships overlaps twice with a single solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))
    table.put(register(pattern1), plan1)

    val patternX = PatternRelationship('r2, ('a, 'b), Direction.OUTGOING, Seq.empty, SimplePatternLength)

    expandSolverStep(qg)(registry, register(pattern1, patternX), table).toSet should equal(Set(
      planSimpleExpand(plan1, 'a, Direction.OUTGOING, 'b, patternX, ExpandInto),
      planSimpleExpand(plan1, 'b, Direction.INCOMING, 'a, patternX, ExpandInto)
    ))
  }

  test("does not expand if an unsolved pattern relationship does not overlap with a solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))
    table.put(register(pattern1), plan1)

    val patternX = PatternRelationship('r2, ('x, 'y), Direction.OUTGOING, Seq.empty, SimplePatternLength)

    expandSolverStep(qg)(registry, register(pattern1, patternX), table).toSet should be(empty)
  }

  test("expands if an unsolved pattern relationship overlaps with multiple solved plans") {
    implicit val registry = IdRegistry[PatternRelationship]

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b, 'c, 'r2, 'd))
    table.put(register(pattern1, pattern2), plan1)

    val pattern3 = PatternRelationship('r3, ('b, 'c), Direction.OUTGOING, Seq.empty, SimplePatternLength)

    expandSolverStep(qg)(registry, register(pattern1, pattern2, pattern3), table).toSet should equal(Set(
      planSimpleExpand(plan1, 'b, Direction.OUTGOING, 'c, pattern3, ExpandInto),
      planSimpleExpand(plan1, 'c, Direction.INCOMING, 'b, pattern3, ExpandInto)
    ))
  }

  def register[X](patRels: X*)(implicit registry: IdRegistry[X]) = registry.registerAll(patRels)
}
