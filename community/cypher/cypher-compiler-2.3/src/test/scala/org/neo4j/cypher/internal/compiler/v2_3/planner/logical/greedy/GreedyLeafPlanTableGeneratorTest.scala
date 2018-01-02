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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy

import org.neo4j.cypher.internal.frontend.v2_3.ast.{Expression, HasLabels, LabelName}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{AllNodesScan, NodeByLabelScan, Selection}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, QueryPlannerConfiguration}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class GreedyLeafPlanTableGeneratorTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  private val solver = GreedyLeafPlanTableGenerator(QueryPlannerConfiguration.default)

  test("single pattern node") {
    new given {
      qg = QueryGraph(patternNodes = Set("n"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      implicit val _ = ctx
      val result = solver.apply(cfg.qg, None)
      val solved = PlannerQuery.empty.withGraph(cfg.qg)

      // then
      result should equal(greedyPlanTableWith(AllNodesScan("n", Set.empty)(solved)))
    }

  }

  test("pattern with two nodes - one allnodes and one labelscan") {
    new given {
      val label: LabelName = LabelName("Label")(pos)
      val hasLabels: Expression = HasLabels(ident("a"), Seq(label))(pos)

      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        selections = Selections.from(hasLabels)
      )
      val solvedA = PlannerQuery.empty.withGraph(QueryGraph(patternNodes = Set("a"), selections = Selections.from(hasLabels)))
      val solvedB = PlannerQuery.empty.withGraph(QueryGraph(patternNodes = Set("b")))

      knownLabels = Set("Label")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      implicit val _ = ctx
      implicit val table = ctx.semanticTable
      import cfg._

      val result = solver.apply(qg, None)

      // then
      result should equal(greedyPlanTableWith(
        NodeByLabelScan("a", LazyLabel(label), Set.empty)(solvedA),
        AllNodesScan("b", Set.empty)(solvedB)
      ))
    }

  }

  test("single node multiple labels") {
    new given {
      val label1 = LabelName("Label1")(pos)
      val label2 = LabelName("Label2")(pos)
      val hasLabels1 = HasLabels(ident("a"), Seq(label1))(pos)
      val hasLabels2 = HasLabels(ident("a"), Seq(label2))(pos)
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        selections = Selections.from(hasLabels1, hasLabels2)
      )
      val solvedA = PlannerQuery.empty.withGraph(QueryGraph(patternNodes = Set("a"), selections = Selections.from(hasLabels2)))
      val solvedAWithLabels1 = solvedA.updateGraph(_.addPredicates(hasLabels1))
      val solvedB = PlannerQuery.empty.withGraph(QueryGraph(patternNodes = Set("b")))

      knownLabels = Set("Label1", "Label2")
      labelCardinality = Map(
        "Label1" -> Cardinality(100),
        "Label2" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      implicit val _ = ctx
      implicit val table = ctx.semanticTable
      import cfg._

      val result = solver.apply(qg, None)

      // then
      result should equal(greedyPlanTableWith(
        Selection(Seq(hasLabels1), NodeByLabelScan("a", LazyLabel(label2), Set.empty)(solvedA))(solvedAWithLabels1),
        AllNodesScan("b", Set.empty)(solvedB)
      ))
    }
  }

  private implicit def lift(plannerQuery: PlannerQuery): PlannerQuery with CardinalityEstimation =
    CardinalityEstimation.lift(plannerQuery, 0.0)
}
