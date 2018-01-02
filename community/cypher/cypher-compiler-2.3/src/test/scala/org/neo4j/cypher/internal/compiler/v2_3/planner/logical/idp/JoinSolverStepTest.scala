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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan, NodeHashJoin, PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, LogicalPlanningContext, Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, LogicalPlanConstructionTestSupport, PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, SemanticTable}

class JoinSolverStepTest extends CypherFunSuite with LogicalPlanConstructionTestSupport {

  private implicit val context = LogicalPlanningContext(mock[PlanContext], LogicalPlanProducer(mock[CardinalityModel]), mock[Metrics], mock[SemanticTable], mock[QueryGraphSolver])

  val plan1 = mock[LogicalPlan]
  val plan2 = mock[LogicalPlan]
  when(plan1.solved).thenReturn(PlannerQuery.empty)
  when(plan2.solved).thenReturn(PlannerQuery.empty)

  val pattern1 = PatternRelationship('r1, ('a, 'b), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val pattern2 = PatternRelationship('r2, ('b, 'c), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  val table = new IDPTable[LogicalPlan]()

  test("does not join based on empty table") {
    implicit val registry = IdRegistry[PatternRelationship]
    val qg = QueryGraph.empty.addPatternNodes('a, 'b, 'c)

    joinSolverStep(qg)(registry, register(pattern1, pattern2), table) should be(empty)
  }

  test("joins plans that solve a single pattern relationship") {
    implicit val registry = IdRegistry[PatternRelationship]
    val qg = QueryGraph.empty.addPatternNodes('a, 'b, 'c)

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))
    when(plan1.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('a, 'b)))
    when(plan2.availableSymbols).thenReturn(Set[IdName]('b, 'r2, 'c))
    when(plan2.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('b, 'c)))

    table.put(register(pattern1), plan1)
    table.put(register(pattern2), plan2)

    joinSolverStep(qg)(registry, register(pattern1, pattern2), table).toSet should equal(Set(
      NodeHashJoin(Set('b), plan1, plan2)(PlannerQuery.empty),
      NodeHashJoin(Set('b), plan2, plan1)(PlannerQuery.empty)
    ))
  }

  test("does not join plans that do not overlap") {
    implicit val registry = IdRegistry[PatternRelationship]
    val qg = QueryGraph.empty.addPatternNodes('a, 'b, 'c, 'd)

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))
    when(plan1.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('a, 'b)))
    when(plan2.availableSymbols).thenReturn(Set[IdName]('c, 'r2, 'd))
    when(plan2.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('c, 'd)))

    table.put(register(pattern1), plan1)
    table.put(register(pattern2), plan2)

    joinSolverStep(qg)(registry, register(pattern1, pattern2), table) should be(empty)
  }


  test("does not join plans that overlap on non-nodes") {
    implicit val registry = IdRegistry[PatternRelationship]
    val qg = QueryGraph.empty.addPatternNodes('a, 'b, 'c, 'd)

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b, 'x))
    when(plan1.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('a, 'b)))
    when(plan2.availableSymbols).thenReturn(Set[IdName]('c, 'r2, 'd, 'x))
    when(plan2.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('c, 'd)))

    table.put(register(pattern1), plan1)
    table.put(register(pattern2), plan2)

    joinSolverStep(qg)(registry, register(pattern1, pattern2), table) should be(empty)
  }

  test("does not join plans that overlap on nodes that are arguments") {
    implicit val registry = IdRegistry[PatternRelationship]
    val qg = QueryGraph.empty.addPatternNodes('a, 'b, 'c, 'd).addArgumentIds(Seq('x))

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b, 'x))
    when(plan1.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('a, 'b, 'x).addArgumentIds(Seq('x))))
    when(plan2.availableSymbols).thenReturn(Set[IdName]('c, 'r2, 'd, 'x))
    when(plan2.solved).thenReturn(PlannerQuery(QueryGraph.empty.addPatternNodes('c, 'd, 'x).addArgumentIds(Seq('x))))

    table.put(register(pattern1), plan1)
    table.put(register(pattern2), plan2)

    joinSolverStep(qg)(registry, register(pattern1, pattern2), table) should be(empty)
  }

  def register[X](patRels: X*)(implicit registry: IdRegistry[X]) = registry.registerAll(patRels)

  private implicit def lift(plannerQuery: PlannerQuery): PlannerQuery with CardinalityEstimation =
    CardinalityEstimation.lift(plannerQuery, Cardinality(0))
}
