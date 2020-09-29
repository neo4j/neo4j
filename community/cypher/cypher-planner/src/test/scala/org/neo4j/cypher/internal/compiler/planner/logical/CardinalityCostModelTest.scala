/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROPERTY_ACCESS_DB_HITS
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.LazyMode
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CardinalityCostModelTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("expand should only be counted once") {
    val cardinalities = new Cardinalities
    val plan =
      setC(Selection(ands(hasLabels("a", "Awesome")),
        setC(Expand(
          setC(Selection(ands(hasLabels("a", "Awesome")),
            setC(Expand(
              setC(Argument(Set("a")), cardinalities, 10.0),
              "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1"), cardinalities, 100.0)
          ), cardinalities, 10.0), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1"), cardinalities, 100.0)
      ), cardinalities, 10.0)

    costFor(plan, QueryGraphSolverInput.empty, SemanticTable(), cardinalities) should equal(Cost(231))
  }

  test("should introduce increase cost when estimating an eager operator and laziness is preferred") {
    val cardinalities = new Cardinalities
    val plan = setC(NodeHashJoin(Set("a"),
      setC(NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone), cardinalities, 10.0),
      setC(Expand(
        setC(NodeByLabelScan("b", labelName("B"), Set.empty, IndexOrderNone), cardinalities, 5.0),
        "b", SemanticDirection.OUTGOING, Seq.empty, "a", "r", ExpandAll), cardinalities, 15.0)
    ), cardinalities, 10.0)

    val pleaseLazy = QueryGraphSolverInput.empty.withPreferredStrictness(LazyMode)
    val whatever = QueryGraphSolverInput.empty

    costFor(plan, whatever, SemanticTable(), cardinalities) should be < costFor(plan, pleaseLazy, SemanticTable(), cardinalities)
  }

  test("non-lazy plan should be penalized when estimating cost wrt a lazy one when laziness is preferred") {
    // MATCH (a1: A)-[r1]->(b)<-[r2]-(a2: A) RETURN b
    val lazyCardinalities = new Cardinalities
    val lazyPlan = setC(Projection(
      setC(Selection(
        Seq(hasLabels("a2", "A")),
        setC(Expand(
          setC(Expand(
            setC(NodeByLabelScan("a1", labelName("A"), Set.empty, IndexOrderNone), lazyCardinalities, 10.0),
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
          setC(NodeByLabelScan("a1", labelName("A"), Set.empty, IndexOrderNone), eagerCardinalities, 10.0),
          "a1", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll
        ), eagerCardinalities, 50.0),
        setC(Expand(
          setC(NodeByLabelScan("a2", labelName("A"), Set.empty, IndexOrderNone), eagerCardinalities, 10.0),
          "a2", SemanticDirection.OUTGOING, Seq.empty, "b", "r2", ExpandAll
        ), eagerCardinalities, 50.0)
      ), eagerCardinalities, 250.0), Map("b" -> varFor("b"))
    ), eagerCardinalities, 250.0)

    val whatever = QueryGraphSolverInput.empty
    costFor(lazyPlan, whatever, SemanticTable(), lazyCardinalities) should be > costFor(eagerPlan, whatever, SemanticTable(), eagerCardinalities)

    val pleaseLazy = QueryGraphSolverInput.empty.withPreferredStrictness(LazyMode)
    costFor(lazyPlan, pleaseLazy, SemanticTable(), lazyCardinalities) should be < costFor(eagerPlan, pleaseLazy, SemanticTable(), eagerCardinalities)
  }

  test("multiple property expressions are counted for in cost") {
    val cardinalities = new Cardinalities
    val cardinality = 10.0
    val plan =
      setC(Selection(List(propEquality("a", "prop1", 42), propEquality("a", "prop1", 43), propEquality("a", "prop1", 44)),
        setC(Argument(Set("a")), cardinalities, cardinality)), cardinalities, cardinality)

    val numberOfPredicates = 3
    val costForSelection = cardinality * numberOfPredicates * PROPERTY_ACCESS_DB_HITS
    val costForArgument = cardinality * .1
    costFor(plan, QueryGraphSolverInput.empty, SemanticTable().addNode(varFor("a")), cardinalities) should equal(Cost(costForSelection + costForArgument))
  }

  test("deeply nested property access does not increase cost") {
    val cardinalities = new Cardinalities
    val cardinality = 10.0

    val shallowPredicate = propEquality("a", "prop1", 42)
    val ap0 = prop("a", "foo")
    val ap1 = prop(ap0, "bar")
    val ap2 = prop(ap1, "baz")
    val ap3 = prop(ap2, "blob")
    val ap4 = prop(ap3, "boing")
    val ap5 = prop(ap4, "peng")
    val ap6 = prop(ap5, "brrt")
    val deepPredicate = equals(ap6, literalInt(2))
    val semanticTable = SemanticTable()
      .addNode(varFor("a"))
      .addTypeInfoCTAny(ap0)
      .addTypeInfoCTAny(ap1)
      .addTypeInfoCTAny(ap2)
      .addTypeInfoCTAny(ap3)
      .addTypeInfoCTAny(ap4)
      .addTypeInfoCTAny(ap5)
      .addTypeInfoCTAny(ap6)

    val plan1 = setC(Selection(List(shallowPredicate), setC(Argument(Set("a")), cardinalities, cardinality)), cardinalities, cardinality)
    val plan2 = setC(Selection(List(deepPredicate), setC(Argument(Set("a")), cardinalities, cardinality)), cardinalities, cardinality)

    costFor(plan1, QueryGraphSolverInput.empty, semanticTable, cardinalities) should equal(costFor(plan2, QueryGraphSolverInput.empty, semanticTable, cardinalities))
  }

  private def costFor(plan: LogicalPlan,
                      input: QueryGraphSolverInput,
                      semanticTable: SemanticTable,
                      cardinalities: Cardinalities): Cost = {
    CardinalityCostModel.costFor(plan, input, semanticTable, cardinalities)
  }

}
