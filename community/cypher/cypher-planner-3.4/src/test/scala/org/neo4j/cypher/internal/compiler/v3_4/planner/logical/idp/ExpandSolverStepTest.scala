/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_4.logical.plans.{Expand, ExpandAll, ExpandInto, LogicalPlan}


class ExpandSolverStepTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  self =>

  implicit def converter(s: Symbol): String = s.toString()

  private val pattern1 = PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val pattern2 = PatternRelationship("r2", ("b", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private val table = new IDPTable[LogicalPlan]()
  private val qg = mock[QueryGraph]

  test("does not expand based on empty table") {
    implicit val registry = IdRegistry[PatternRelationship]
    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      expandSolverStep(qg)(registry, register(pattern1, pattern2), table, ctx, solveds) should be(empty)
    }
  }

  test("expands if an unsolved pattern relationship overlaps once with a single solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan1 = fakeLogicalPlanFor("a", "r1", "b")
      solveds.set(plan1.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("a", "b")))
      table.put(register(pattern1), plan1)

      expandSolverStep(qg)(registry, register(pattern1, pattern2), table, ctx, solveds).toSet should equal(Set(
        Expand(plan1, "b", SemanticDirection.OUTGOING, Seq.empty, "c", "r2", ExpandAll)
      ))
    }
  }

  test("expands if an unsolved pattern relationships overlaps twice with a single solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan1 = fakeLogicalPlanFor("a", "r1", "b")
      solveds.set(plan1.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("a", "b")))
      table.put(register(pattern1), plan1)

      val patternX = PatternRelationship("r2", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

      expandSolverStep(qg)(registry, register(pattern1, patternX), table, ctx, solveds).toSet should equal(Set(
        Expand(plan1, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r2", ExpandInto),
        Expand(plan1, "b", SemanticDirection.INCOMING, Seq.empty, "a", "r2", ExpandInto)
      ))
    }
  }

  test("does not expand if an unsolved pattern relationship does not overlap with a solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]
    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan1 = fakeLogicalPlanFor("a", "r1", "b")
      solveds.set(plan1.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("a", "b")))
      table.put(register(pattern1), plan1)

      val patternX = PatternRelationship("r2", ("x", "y"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

      expandSolverStep(qg)(registry, register(pattern1, patternX), table, ctx, solveds).toSet should be(empty)
    }

  }

  test("expands if an unsolved pattern relationship overlaps with multiple solved plans") {
    implicit val registry = IdRegistry[PatternRelationship]

    new given().withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan1 = fakeLogicalPlanFor("a", "r1", "b", "c", "r2", "d")
      solveds.set(plan1.id, RegularPlannerQuery(QueryGraph.empty.addPatternNodes("a", "b", "c", "d")))
      table.put(register(pattern1, pattern2), plan1)

      val pattern3 = PatternRelationship("r3", ("b", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

      expandSolverStep(qg)(registry, register(pattern1, pattern2, pattern3), table, ctx, solveds).toSet should equal(Set(
        Expand(plan1, "b", SemanticDirection.OUTGOING, Seq.empty, "c", "r3", ExpandInto),
        Expand(plan1, "c", SemanticDirection.INCOMING, Seq.empty, "b", "r3", ExpandInto)
      ))
    }
  }

  def register[X](patRels: X*)(implicit registry: IdRegistry[X]) = registry.registerAll(patRels)
}
