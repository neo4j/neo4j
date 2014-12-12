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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.{LabelName, HasLabels}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.ExhaustiveQueryGraphSolver.PlanProducer
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Selections, LogicalPlanningTestSupport2, PlannerQuery, QueryGraph}
import org.neo4j.graphdb.Direction

import scala.collection.{immutable, Map}

class ExhaustiveQueryGraphSolverTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan for a single node pattern") {
    val solver = ExhaustiveQueryGraphSolver(
      generatePlanTable(AllNodesScan("a", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a"))))),
      Seq(undefinedPlanProducer))

    new given {
      qg = QueryGraph(patternNodes = Set("a"))
      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        solver.tryPlan(qg).get should equal(
          AllNodesScan("a", Set.empty)(null)
        )
      }
    }
  }

  test("should plan for a single relationship pattern") {
    val solver = ExhaustiveQueryGraphSolver(generatePlanTable(
      NodeByLabelScan("a", Left("A"), Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a"), selections = Selections.from(
        HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos))))),
      NodeByLabelScan("b", Left("B"), Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("b"), selections = Selections.from(
        HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos)))))
    ), Seq(expandOptions))

    new given {
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(
          HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos),
          HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "A" -> Cardinality(1000),
        "B" -> Cardinality(10)
      )

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        solver.tryPlan(qg).get should equal(
          Expand(NodeByLabelScan("a", Left("A"), Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "b", "r")(null)
        )
      }
    }
  }

  private val undefinedPlanProducer: PlanProducer = new PlanProducer {
    def apply(qg: QueryGraph, cache: Map[QueryGraph, LogicalPlan]): Seq[LogicalPlan] = ???
  }

  private def generatePlanTable(plans: LogicalPlan*): PlanTableGenerator = {
    new PlanTableGenerator {
      def apply(qg: QueryGraph, plan: Option[LogicalPlan])(implicit context: LogicalPlanningContext): PlanTable =
        PlanTable(plans: _*)
    }
  }
}
