/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical

import org.neo4j.cypher.internal.compiler.v3_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_1.ast.{AstConstructionTestSupport, HasLabels, LabelName}
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class CardinalityCostModelTest extends CypherFunSuite with LogicalPlanningTestSupport with AstConstructionTestSupport {

  test("expand should only be counted once") {
    val plan =
      Selection(List(HasLabels(varFor("a"), Seq(LabelName("Awesome") _)) _),
        Expand(
          Selection(List(HasLabels(varFor("a"), Seq(LabelName("Awesome") _)) _),
            Expand(
              Argument(Set("a"))(solvedWithEstimation(10.0))(),
              "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1")(solvedWithEstimation(100.0))
          )(solvedWithEstimation(10.0)), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1")(solvedWithEstimation(100.0))
      )(solvedWithEstimation(10.0))

    CardinalityCostModel(plan, QueryGraphSolverInput.empty) should equal(Cost(231))
  }

  test("should introduce increase cost when estimating an eager operator and lazyness is preferred") {
    val plan = NodeHashJoin(Set("a"),
      NodeByLabelScan("a", lblName("A"), Set.empty)(solvedWithEstimation(10.0)),
      Expand(
        NodeByLabelScan("b", lblName("B"), Set.empty)(solvedWithEstimation(5.0)),
        "b", SemanticDirection.OUTGOING, Seq.empty, "a", "r", ExpandAll)(solvedWithEstimation(15.0))
    )(solvedWithEstimation(10.0))

    val pleaseLazy = QueryGraphSolverInput.empty.withPreferredStrictness(LazyMode)
    val whatever = QueryGraphSolverInput.empty

    CardinalityCostModel(plan, whatever) should be < CardinalityCostModel(plan, pleaseLazy)
  }

  test("non-lazy plan should be penalized when estimating cost wrt a lazy one when lazyness is preferred") {
    // MATCH (a1: A)-[r1]->(b)<-[r2]-(a2: A) RETURN b

    val lazyPlan = Projection(
      Selection(
        Seq(HasLabels(varFor("a2"), Seq(LabelName("A")(pos)))(pos)),
        Expand(
          Expand(
            NodeByLabelScan("a1", lblName("A"), Set.empty)(solvedWithEstimation(10.0)),
            "a1", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll
          )(solvedWithEstimation(50.0)),
          "b", SemanticDirection.INCOMING, Seq.empty, "a2", "r2", ExpandAll
        )(solvedWithEstimation(250.0))
      )(solvedWithEstimation(250.0)), Map("b" -> varFor("b"))
    )(solvedWithEstimation(250.0))

    val eagerPlan = Projection(
      NodeHashJoin(Set("b"),
        Expand(
          NodeByLabelScan("a1", lblName("A"), Set.empty)(solvedWithEstimation(10.0)),
          "a1", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll
        )(solvedWithEstimation(50.0)),
        Expand(
          NodeByLabelScan("a2", lblName("A"), Set.empty)(solvedWithEstimation(10.0)),
          "a2", SemanticDirection.OUTGOING, Seq.empty, "b", "r2", ExpandAll
        )(solvedWithEstimation(50.0))
      )(solvedWithEstimation(250.0)), Map("b" -> varFor("b"))
    )(solvedWithEstimation(250.0))

    val whatever = QueryGraphSolverInput.empty
    CardinalityCostModel(lazyPlan, whatever) should be > CardinalityCostModel(eagerPlan, whatever)

    val pleaseLazy = QueryGraphSolverInput.empty.withPreferredStrictness(LazyMode)
    CardinalityCostModel(lazyPlan, pleaseLazy) should be < CardinalityCostModel(eagerPlan, pleaseLazy)
  }

  test("multiple property expressions are counted for in cost") {
    val cardinality = 10.0
    val card10 = solvedWithEstimation(cardinality)
    val plan =
      Selection(List(propEquality("a", "prop1", 42), propEquality("a", "prop1", 42), propEquality("a", "prop1", 42)),
        Argument(Set("a"))(card10)())(card10)

    val numberOfPredicates = 3
    val costForSelection = cardinality * numberOfPredicates
    val costForArgument = cardinality *   .1
    CardinalityCostModel(plan, QueryGraphSolverInput.empty) should equal(Cost(costForSelection + costForArgument))
  }

}
