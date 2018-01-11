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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp

import org.mockito.Mockito._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{LogicalPlanningContext, Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.v3_4.logical.plans.{Expand, ExpandAll, ExpandInto, LogicalPlan}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.util.v3_4.Cardinality
import org.neo4j.cypher.internal.util.v3_4.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection

class ExpandSolverStepTest extends CypherFunSuite with LogicalPlanConstructionTestSupport with AstConstructionTestSupport {
  self =>
  private val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(0))

  case class TestPlan(availableSymbols: Set[IdName] = Set.empty) extends LogicalPlan(new SequentialIdGen) {

    override def lhs: Option[LogicalPlan] = None

    override def rhs: Option[LogicalPlan] = None

    override def solved: PlannerQuery with CardinalityEstimation = self.solved

    override def strictness: StrictnessMode = ???
  }

  private val plan1 = TestPlan()


  private val pattern1 = PatternRelationship('r1, ('a, 'b), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val pattern2 = PatternRelationship('r2, ('b, 'c), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private val table = new IDPTable[LogicalPlan]()
  private val qg = mock[QueryGraph]

  private val context = LogicalPlanningContext(mock[PlanContext], LogicalPlanProducer(mock[CardinalityModel], LogicalPlan.LOWEST_TX_LAYER, idGen),
    mock[Metrics], mock[SemanticTable], mock[QueryGraphSolver], notificationLogger = mock[InternalNotificationLogger])

  test("does not expand based on empty table") {
    implicit val registry = IdRegistry[PatternRelationship]

    expandSolverStep(qg)(registry, register(pattern1, pattern2), table, context) should be(empty)
  }

  test("expands if an unsolved pattern relationship overlaps once with a single solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    val plan = TestPlan(Set[IdName]('a, 'r1, 'b))
    table.put(register(pattern1), plan)

    expandSolverStep(qg)(registry, register(pattern1, pattern2), table, context).toSet should equal(Set(
      Expand(plan, 'b, SemanticDirection.OUTGOING, Seq.empty, 'c, 'r2, ExpandAll)(solved)
    ))
  }

  test("expands if an unsolved pattern relationships overlaps twice with a single solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    val plan = TestPlan(Set[IdName]('a, 'r1, 'b))

    table.put(register(pattern1), plan)

    val patternX = PatternRelationship('r2, ('a, 'b), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    expandSolverStep(qg)(registry, register(pattern1, patternX), table, context).toSet should equal(Set(
      Expand(plan, 'a, SemanticDirection.OUTGOING, Seq.empty, 'b, 'r2, ExpandInto)(solved),
      Expand(plan, 'b, SemanticDirection.INCOMING, Seq.empty, 'a, 'r2, ExpandInto)(solved)
    ))
  }

  test("does not expand if an unsolved pattern relationship does not overlap with a solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    when(plan1.availableSymbols).thenReturn(Set[IdName]('a, 'r1, 'b))
    table.put(register(pattern1), plan1)

    val patternX = PatternRelationship('r2, ('x, 'y), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    expandSolverStep(qg)(registry, register(pattern1, patternX), table, context).toSet should be(empty)
  }

  test("expands if an unsolved pattern relationship overlaps with multiple solved plans") {
    implicit val registry = IdRegistry[PatternRelationship]
    val plan = TestPlan(Set[IdName]('a, 'r1, 'b, 'c, 'r2, 'd))

    table.put(register(pattern1, pattern2), plan)

    val pattern3 = PatternRelationship('r3, ('b, 'c), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    expandSolverStep(qg)(registry, register(pattern1, pattern2, pattern3), table, context).toSet should equal(Set(
      Expand(plan, 'b, SemanticDirection.OUTGOING, Seq.empty, 'c, 'r3, ExpandInto)(solved),
      Expand(plan, 'c, SemanticDirection.INCOMING, Seq.empty, 'b, 'r3, ExpandInto)(solved)
    ))
  }

  def register[X](patRels: X*)(implicit registry: IdRegistry[X]) = registry.registerAll(patRels)
}
