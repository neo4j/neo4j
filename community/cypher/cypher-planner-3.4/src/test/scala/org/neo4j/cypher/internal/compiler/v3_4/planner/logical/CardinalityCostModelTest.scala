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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.ir.v3_4.LazyMode
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, Cost}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.{HasLabels, LabelName, SemanticDirection}
import org.neo4j.cypher.internal.v3_4.logical.plans._

class CardinalityCostModelTest extends CypherFunSuite with LogicalPlanningTestSupport2  {

  test("expand should only be counted once") {
    val cardinalities = new Cardinalities
    val plan =
      setC(Selection(List(HasLabels(varFor("a"), Seq(LabelName("Awesome") _)) _),
        setC(Expand(
          setC(Selection(List(HasLabels(varFor("a"), Seq(LabelName("Awesome") _)) _),
            setC(Expand(
              setC(Argument(Set("a")), cardinalities, 10.0),
              "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1"), cardinalities, 100.0)
          ), cardinalities, 10.0), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1"), cardinalities, 100.0)
      ), cardinalities, 10.0)

    CardinalityCostModel(plan, QueryGraphSolverInput.empty, cardinalities) should equal(Cost(231))
  }

  test("should introduce increase cost when estimating an eager operator and lazyness is preferred") {
    val cardinalities = new Cardinalities
    val plan = setC(NodeHashJoin(Set("a"),
      setC(NodeByLabelScan("a", lblName("A"), Set.empty), cardinalities, 10.0),
      setC(Expand(
        setC(NodeByLabelScan("b", lblName("B"), Set.empty), cardinalities, 5.0),
        "b", SemanticDirection.OUTGOING, Seq.empty, "a", "r", ExpandAll), cardinalities, 15.0)
    ), cardinalities, 10.0)

    val pleaseLazy = QueryGraphSolverInput.empty.withPreferredStrictness(LazyMode)
    val whatever = QueryGraphSolverInput.empty

    CardinalityCostModel(plan, whatever, cardinalities) should be < CardinalityCostModel(plan, pleaseLazy, cardinalities)
  }

  test("non-lazy plan should be penalized when estimating cost wrt a lazy one when lazyness is preferred") {
    // MATCH (a1: A)-[r1]->(b)<-[r2]-(a2: A) RETURN b
    val lazyCardinalities = new Cardinalities
    val lazyPlan = setC(Projection(
      setC(Selection(
        Seq(HasLabels(varFor("a2"), Seq(LabelName("A")(pos)))(pos)),
        setC(Expand(
          setC(Expand(
            setC(NodeByLabelScan("a1", lblName("A"), Set.empty), lazyCardinalities, 10.0),
            "a1", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll
          ), lazyCardinalities, 50.0),
          "b", SemanticDirection.INCOMING, Seq.empty, "a2", "r2", ExpandAll
        ), lazyCardinalities, 250.0)
      ), lazyCardinalities, 250.0), Map("b" -> varFor("b"))
    ), lazyCardinalities, 250.0)

    val eagerCardinalities = new Cardinalities
    val eagerPlan = setC(Projection(
      setC(NodeHashJoin(Set("b"),
        setC(Expand(
          setC(NodeByLabelScan("a1", lblName("A"), Set.empty), eagerCardinalities, 10.0),
          "a1", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll
        ), eagerCardinalities, 50.0),
        setC(Expand(
          setC(NodeByLabelScan("a2", lblName("A"), Set.empty), eagerCardinalities, 10.0),
          "a2", SemanticDirection.OUTGOING, Seq.empty, "b", "r2", ExpandAll
        ), eagerCardinalities, 50.0)
      ), eagerCardinalities, 250.0), Map("b" -> varFor("b"))
    ), eagerCardinalities, 250.0)

    val whatever = QueryGraphSolverInput.empty
    CardinalityCostModel(lazyPlan, whatever, lazyCardinalities) should be > CardinalityCostModel(eagerPlan, whatever, eagerCardinalities)

    val pleaseLazy = QueryGraphSolverInput.empty.withPreferredStrictness(LazyMode)
    CardinalityCostModel(lazyPlan, pleaseLazy, lazyCardinalities) should be < CardinalityCostModel(eagerPlan, pleaseLazy, eagerCardinalities)
  }

  test("multiple property expressions are counted for in cost") {
    val cardinalities = new Cardinalities
    val cardinality = 10.0
    val plan =
      setC(Selection(List(propEquality("a", "prop1", 42), propEquality("a", "prop1", 42), propEquality("a", "prop1", 42)),
        setC(Argument(Set("a")), cardinalities, cardinality)), cardinalities, cardinality)

    val numberOfPredicates = 3
    val costForSelection = cardinality * numberOfPredicates
    val costForArgument = cardinality *   .1
    CardinalityCostModel(plan, QueryGraphSolverInput.empty, cardinalities) should equal(Cost(costForSelection + costForArgument))
  }

}
