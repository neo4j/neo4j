/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanResolver
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.ProcedureTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType
import org.scalatest.Assertion

class UnnestApplyTest extends CypherFunSuite with LogicalPlanningAttributesTestSupport
    with LogicalPlanConstructionTestSupport with AstConstructionTestSupport with ProcedureTestSupport {

  private val po_n: ProvidedOrder = DefaultProvidedOrderFactory.asc(v"n")

  test("should unnest apply with a single Argument on the lhs") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20).withProvidedOrder(po_n)
      .|.nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      .argument().withCardinality(1)

    inputBuilder shouldRewriteToPlanWithAttributes (new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n))
  }

  test("should unnest apply with a single Argument on the rhs") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20).withProvidedOrder(po_n)
      .apply().withCardinality(20).withProvidedOrder(po_n)
      .|.argument().withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20).withProvidedOrder(po_n)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n))
  }

  test("should not cross OPTIONAL boundaries") {
    val input = new LogicalPlanBuilder()
      .produceResults("x", "n")
      .apply()
      .|.optional("n")
      .|.filter("n.prop = 42")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(input)
  }

  test("apply on apply should be extracted nicely") {
    /*
                            Apply1
                         LHS   Apply2       =>  Expand
                             Arg1  Expand        LHS
                                      Arg2
     */

    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m", "o").withCardinality(300).withProvidedOrder(po_n)
      .apply().withCardinality(300).withProvidedOrder(po_n)
      .|.apply().withCardinality(3)
      .|.|.expand("(n)-->(m)").withCardinality(6)
      .|.|.argument("n").withCardinality(1)
      .|.filter("n.prop = 42").withCardinality(0.5)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m", "o").withCardinality(300).withProvidedOrder(po_n)
        .expand("(n)-->(m)").withCardinality(300).withProvidedOrder(po_n)
        .filter("n.prop = 42").withCardinality(50).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("apply on apply should be extracted nicely 2") {
    /*  Moves filtering on a LHS of inner Apply to the LHS of the outer

                            Apply1                      Apply1
                         LHS1     Apply2       =>  Filter    Apply2
                              Filter   RHS        LHS1     LHS2   RHS
                           LHS2
     */

    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(40000).withProvidedOrder(po_n)
      .apply().withCardinality(40000).withProvidedOrder(po_n)
      .|.apply().withCardinality(400)
      .|.|.nodeByLabelScan("o", "O", "n", "m").withCardinality(40)
      .|.filter("n.prop > 10").withCardinality(10)
      .|.nodeByLabelScan("m", "M", "n").withCardinality(20)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(40000).withProvidedOrder(po_n)
      .apply().withCardinality(40000).withProvidedOrder(po_n)
      .|.apply().withCardinality(800)
      .|.|.nodeByLabelScan("o", "O", "n", "m").withCardinality(40)
      .|.nodeByLabelScan("m", "M", "n").withCardinality(20)
      .filter("n.prop > 10").withCardinality(50).withProvidedOrder(po_n)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n))
  }

  test("apply on apply should be left unchanged") {
    /*  Does not moves filtering on a LHS of inner Apply to the LHS of the outer if Selection has a dependency on a
    variable introduced in the source operator

                            Apply1
                         LHS1     Apply2       =>  remains unchanged
                              Filter   RHS
                           LHS2
     */

    val input = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(40000)
      .apply().withCardinality(40000)
      .|.apply().withCardinality(400)
      .|.|.nodeByLabelScan("o", "O", "n", "m").withCardinality(40)
      .|.filter("m.prop > 10").withCardinality(10)
      .|.nodeByLabelScan("m", "M", "n").withCardinality(20)
      .allNodeScan("n").withCardinality(100)
      .build()

    rewrite(input) should equal(input)
  }

  test("apply on apply on optional should be OK") {
    /*
                            Apply1                Apply
                         LHS   Apply2       =>  LHS Optional
                             Arg1  Optional             Expand
                                     Expand                Arg
                                       Arg2
     */

    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(1000).withProvidedOrder(po_n)
      .apply().withCardinality(1000).withProvidedOrder(po_n)
      .|.apply().withCardinality(10)
      .|.|.optional("n").withCardinality(10)
      .|.|.expand("(n)-->(m)").withCardinality(15)
      .|.|.argument("n").withCardinality(1)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(1000).withProvidedOrder(po_n)
      .apply().withCardinality(1000).withProvidedOrder(po_n)
      .|.optional("n").withCardinality(10)
      .|.expand("(n)-->(m)").withCardinality(15)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n))
  }

  test("AntiConditionalApply on apply on optional should be OK") {
    /*
                            ACA                  ACA
                         LHS   Apply       =>  LHS Optional
                             Arg1  Optional             Expand
                                     Expand                Arg
                                       Arg2
     */

    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(1000).withProvidedOrder(po_n)
      .antiConditionalApply("n").withCardinality(1000).withProvidedOrder(po_n)
      .|.apply().withCardinality(10)
      .|.|.optional("n").withCardinality(10)
      .|.|.expand("(n)-->(m)").withCardinality(15)
      .|.|.argument("n").withCardinality(1)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(1000).withProvidedOrder(po_n)
      .antiConditionalApply("n").withCardinality(1000).withProvidedOrder(po_n)
      .|.optional("n").withCardinality(10)
      .|.expand("(n)-->(m)").withCardinality(15)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n))
  }

  test("π (Arg) Ax R => π (R)") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(100)
      .apply().withCardinality(100)
      .|.allNodeScan("n", "x").withCardinality(100).withProvidedOrder(po_n)
      .projection("5 AS x").withCardinality(1)
      .argument().withCardinality(1)

    inputBuilder shouldRewriteToPlanWithAttributes (new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(100)
      .projection("5 as x").withCardinality(100).withProvidedOrder(po_n)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n))
  }

  test("π (Arg) Ax R => π (R): keeps arguments if nested under another apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(50)
      .semiApply().withCardinality(50)
      .|.apply().withCardinality(100)
      .|.|.allNodeScan("n", "x", "m").withCardinality(100).withProvidedOrder(po_n)
      .|.projection("5 AS x").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .allNodeScan("m").withCardinality(100)

    inputBuilder shouldRewriteToPlanWithAttributes (new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(50)
      .semiApply().withCardinality(50)
      .|.projection("5 as x").withCardinality(100).withProvidedOrder(po_n)
      .|.allNodeScan("n", "m").withCardinality(100).withProvidedOrder(po_n)
      .allNodeScan("m").withCardinality(100))
  }

  test("π (Arg) Ax R => π (R): passes dependencies of projection") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(50)
      .semiApply().withCardinality(50)
      .|.apply().withCardinality(100)
      .|.|.allNodeScan("n", "x").withCardinality(100).withProvidedOrder(po_n)
      .|.projection("m AS x").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .allNodeScan("m").withCardinality(100)

    inputBuilder shouldRewriteToPlanWithAttributes (new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(50)
      .semiApply().withCardinality(50)
      .|.projection("m as x").withCardinality(100).withProvidedOrder(po_n)
      .|.allNodeScan("n", "m").withCardinality(100).withProvidedOrder(po_n)
      .allNodeScan("m").withCardinality(100))
  }

  test("π (Arg) Ax R => π (R): if R uses projected value") {
    val input = new LogicalPlanBuilder()
      .produceResults("x", "n")
      .apply()
      .|.nodeIndexOperator(
        "n:Label(prop=???)",
        paramExpr = Some(v"x"),
        argumentIds = Set("x"),
        indexType = IndexType.RANGE
      )
      .projection("5 AS x")
      .argument()
      .build()

    rewrite(input) should equal(input)
  }

  test("should unnest Expand and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
      .apply().withCardinality(200).withProvidedOrder(po_n)
      .|.expand("(n)-->(m)").withCardinality(2)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
        .expand("(n)-->(m)").withCardinality(200).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest two Expands and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m", "o").withCardinality(400).withProvidedOrder(po_n)
      .apply().withCardinality(400).withProvidedOrder(po_n)
      .|.expand("(m)-->(o)").withCardinality(4)
      .|.expand("(n)-->(m)").withCardinality(2)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m", "o").withCardinality(400).withProvidedOrder(po_n)
        .expand("(m)-->(o)").withCardinality(400).withProvidedOrder(po_n)
        .expand("(n)-->(m)").withCardinality(200).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest StatefulShortestPath and multiply cardinality") {
    val nfa = new TestNFABuilder(0, "n")
      .addTransition(0, 1, "(n) (n_i)")
      .addTransition(1, 2, "(n_i)-[r_i]->(m_i)")
      .addTransition(2, 1, "(m_i) (n_i)")
      .addTransition(2, 3, "(m_i) (m)")
      .setFinalState(3)
      .build()

    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m", "o").withCardinality(200).withProvidedOrder(po_n)
      .apply().withCardinality(200).withProvidedOrder(po_n)
      .|.statefulShortestPath(
        "n",
        "m",
        "",
        None,
        groupNodes = Set(("n_i", "n_i"), ("m_i", "m_i")),
        groupRelationships = Set(("r_i", "r_i")),
        singletonNodeVariables = Set("m" -> "m"),
        singletonRelationshipVariables = Set.empty,
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      ).withCardinality(2)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m", "o").withCardinality(200).withProvidedOrder(po_n)
        .statefulShortestPath(
          "n",
          "m",
          "",
          None,
          groupNodes = Set(("n_i", "n_i"), ("m_i", "m_i")),
          groupRelationships = Set(("r_i", "r_i")),
          singletonNodeVariables = Set("m" -> "m"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          false
        ).withCardinality(200).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest Expand but keep Apply and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
      .apply().withCardinality(200).withProvidedOrder(po_n)
      .|.expandInto("(n)-->(m)").withCardinality(2)
      .|.allNodeScan("m").withCardinality(1000)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
        .expandInto("(n)-->(m)").withCardinality(200).withProvidedOrder(po_n)
        .apply().withCardinality(100000).withProvidedOrder(po_n)
        .|.allNodeScan("m").withCardinality(1000)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest Selection and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(50).withProvidedOrder(po_n)
      .apply().withCardinality(50).withProvidedOrder(po_n)
      .|.filter("n.prop > 0").withCardinality(0.5)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n").withCardinality(50).withProvidedOrder(po_n)
        .filter("n.prop > 0").withCardinality(50).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest Projection and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(50).withProvidedOrder(po_n)
      .apply().withCardinality(50).withProvidedOrder(po_n)
      .|.projection("n AS m").withCardinality(1)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(50).withProvidedOrder(po_n)
        .projection("n AS m").withCardinality(100).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest VarExpand and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
      .apply().withCardinality(200).withProvidedOrder(po_n)
      .|.expand("(n)-[*]->(m)").withCardinality(2)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
        .expand("(n)-[*]->(m)").withCardinality(200).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest OptionalExpand and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
      .apply().withCardinality(200).withProvidedOrder(po_n)
      .|.optionalExpandAll("(n)-->(m)").withCardinality(2)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
        .optionalExpandAll("(n)-->(m)").withCardinality(200).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should not unnest OptionalExpand on top of non-Argument") {
    val input = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .apply().withCardinality(400)
      .|.optionalExpandAll("(n)-->(o)")
      .|.optionalExpandAll("(n)-->(m)")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(input)
  }

  test("should unnest nested Apply with Expand on LHS") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(1000).withProvidedOrder(po_n)
      .apply().withCardinality(1000).withProvidedOrder(po_n)
      .|.apply().withCardinality(10)
      .|.|.limit(1).withCardinality(1)
      .|.|.argument("n").withCardinality(1)
      .|.expand("(m)--(n)").withCardinality(10)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(1000).withProvidedOrder(po_n)
        .apply().withCardinality(1000).withProvidedOrder(po_n)
        .|.limit(1).withCardinality(1)
        .|.argument("n").withCardinality(1)
        .expand("(m)--(n)").withCardinality(1000).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest nested SemiApply with Expand on LHS") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(500).withProvidedOrder(po_n)
      .apply().withCardinality(500).withProvidedOrder(po_n)
      .|.semiApply().withCardinality(5)
      .|.|.limit(1).withCardinality(1)
      .|.|.argument("n").withCardinality(1)
      .|.expand("(m)--(n)").withCardinality(10)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(500).withProvidedOrder(po_n)
        .semiApply().withCardinality(500).withProvidedOrder(po_n)
        .|.limit(1).withCardinality(1)
        .|.argument("n").withCardinality(1)
        .expand("(m)--(n)").withCardinality(1000).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest nested Apply with deeply nested unary plan on LHS") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(1000).withProvidedOrder(po_n)
      .apply().withCardinality(1000).withProvidedOrder(po_n)
      .|.apply().withCardinality(10)
      .|.|.limit(1).withCardinality(1)
      .|.|.argument("n").withCardinality(1)
      .|.filter("m.prop > 0").withCardinality(10)
      .|.expand("(n)--(m)").withCardinality(15)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes (
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(1000).withProvidedOrder(po_n)
        .apply().withCardinality(1000).withProvidedOrder(po_n)
        .|.limit(1).withCardinality(1)
        .|.argument("n").withCardinality(1)
        .filter("m.prop > 0").withCardinality(1000).withProvidedOrder(po_n)
        .expand("(n)--(m)").withCardinality(1500).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should not unnest nested Apply with Distinct on LHS") {
    val input = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .apply()
      .|.apply()
      .|.|.limit(1)
      .|.|.argument("n")
      .|.distinct("m AS m")
      .|.expand("(m)--(n)")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(input)
  }

  test("should not unnest nested Apply with Sort on LHS") {
    val input = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .apply()
      .|.apply()
      .|.|.limit(1)
      .|.|.argument("n")
      .|.sort("n ASC")
      .|.expand("(m)--(n)")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(input)
  }

  test("should remove apply with argument on LHS, even when RHS has updates") {
    val input = new LogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.setLabels("n", "Label")
      .|.allNodeScan("n")
      .argument()
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("n")
      .setLabels("n", "Label")
      .allNodeScan("n")
      .build()

    rewrite(input) should equal(expected)
  }

  test("should remove apply with argument on RHS") {
    val input = new LogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("n")
      .allNodeScan("n")
      .build()

    rewrite(input) should equal(expected)
  }

  test("should not unnest updating plan") {
    val input = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .apply()
      .|.setLabels("m", "Label")
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()

    rewrite(input) should equal(input)
  }

  test("should not unnest non-updating plan if RHS contains updates") {
    val input = new LogicalPlanBuilder()
      .produceResults("n", "prop")
      .apply()
      .|.projection("m.prop AS prop")
      .|.setLabels("m", "Label")
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()

    rewrite(input) should equal(input)
  }

  test("should unnest nested applies") {
    val input = new LogicalPlanBuilder()
      .produceResults("n", "m", "o")
      .apply()
      .|.apply()
      .|.|.expand("(n)-->(m)")
      .|.|.argument("n")
      .|.filter("n.prop = 42")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("n", "m", "o")
      .expandAll("(n)-[UNNAMED1]->(m)")
      .filter("n.prop = 42")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(expected)
  }

  test("should not unnest ForeachApply") {
    val input = new LogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.foreachApply("n", "[1, 2, 3]")
      .|.|.setProperty("n", "prop", "5")
      .|.|.argument("n")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(input)
  }

  test("should not unnest nested Apply with updates") {
    val input = new LogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.apply()
      .|.|.setProperty("m", "prop", "5")
      .|.|.argument("m")
      .|.filter("m.prop > 0")
      .|.expand("(n)--(m)")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(input)
  }

  test("should not unnest nested Apply with write procedure call") {
    val resolver = new LogicalPlanResolver(
      procedures = Set(
        procedureSignature("my.writeProc")
          .withAccessMode(ProcedureReadWriteAccess)
          .build()
      )
    )
    val input = new LogicalPlanBuilder(resolver = resolver)
      .produceResults("n")
      .apply()
      .|.apply()
      .|.|.procedureCall("my.writeProc()")
      .|.|.argument("m")
      .|.filter("m.prop > 0")
      .|.expand("(n)--(m)")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(input)
  }

  test("should not unnest Apply with projection on LHS when RHS has updates") {
    val input = new LogicalPlanBuilder()
      .produceResults("x", "n")
      .apply()
      .|.setProperty("n", "prop", "1")
      .|.allNodeScan("n", "x")
      .projection("5 AS x")
      .argument()
      .build()

    rewrite(input) should equal(input)
  }

  test("should not unnest Apply with lhs Argument if RHS has aggregation") {
    val input = new LogicalPlanBuilder()
      .produceResults("p", "c")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS c2"))
      .|.filter("p.prop > 5")
      .|.apply() // Must not be removed - otherwise `p` will be unavailable for the filter above
      .|.|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.|.allNodeScan("n", "p")
      .|.argument("p")
      .allNodeScan("p")
      .build()

    rewrite(input) should equal(input)
  }

  test("should not unnest Apply with lhs Argument if RHS has distinct") {
    val input = new LogicalPlanBuilder()
      .produceResults("p", "c")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS c2"))
      .|.filter("p.prop > 5")
      .|.apply() // Must not be removed - otherwise `p` will be unavailable for the filter above
      .|.|.distinct("n AS n")
      .|.|.allNodeScan("n", "p")
      .|.argument("p")
      .allNodeScan("p")
      .build()

    rewrite(input) should equal(input)
  }

  test("should unnest Apply with lhs Argument if RHS has fewer arguments, but keeps all arguments") {
    val input = new LogicalPlanBuilder()
      .produceResults("p", "c")
      .apply() // Can be un-nested even though ...
      .|.merge(Seq(), Seq(createRelationship("anon_0", "o", "KNOWS", "a", OUTGOING)), lockNodes = Set("o", "a"))
      .|.expandInto("(o)-[anon_0:KNOWS]->(a)")
      .|.argument("a", "o") // ... RHS does not use `others`.
      .argument("a", "o", "others")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("p", "c")
      .merge(Seq(), Seq(createRelationship("anon_0", "o", "KNOWS", "a", OUTGOING)), lockNodes = Set("o", "a"))
      .expandInto("(o)-[anon_0:KNOWS]->(a)")
      .argument("a", "o", "others")
      .build()

    rewrite(input) should equal(expected)
  }

  test("should not unnest Apply with lhs Projection if it is not a simple projection") {
    val input = new LogicalPlanBuilder()
      .produceResults("a", "x")
      .apply()
      .|.limit(1)
      .|.filter("a.prop = prop")
      .|.apply()
      .|.|.allRelationshipsScan("(a)-[anon_0]-(anon_1)")
      .|.projection("x.prop AS prop")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    rewrite(input) shouldEqual input
  }

  implicit private class AssertableInputBuilder(inputBuilder: LogicalPlanBuilder) {

    def shouldRewriteToPlanWithAttributes(expectedBuilder: LogicalPlanBuilder): Assertion = {
      val resultPlan =
        rewrite(inputBuilder.build(), inputBuilder.cardinalities, inputBuilder.providedOrders, inputBuilder.idGen)
      (resultPlan, inputBuilder.cardinalities) should haveSamePlanAndCardinalitiesAs((
        expectedBuilder.build(),
        expectedBuilder.cardinalities
      ))
      (resultPlan, inputBuilder.providedOrders) should haveSamePlanAndProvidedOrdersAs((
        expectedBuilder.build(),
        expectedBuilder.providedOrders
      ))
    }
  }

  private def rewrite(
    p: LogicalPlan,
    cardinalities: Cardinalities,
    providedOrders: ProvidedOrders,
    idGen: IdGen
  ): LogicalPlan = {
    val solveds = new StubSolveds
    val unnest =
      UnnestApply(solveds, cardinalities, providedOrders, Attributes(idGen), CancellationChecker.neverCancelled())

    p.endoRewrite(unnest)
  }

  private def stubCardinalities(): StubCardinalities = new StubCardinalities {
    override def defaultValue: Cardinality = Cardinality.SINGLE
  }

  private def rewrite(p: LogicalPlan): LogicalPlan = rewrite(p, stubCardinalities(), new StubProvidedOrders, idGen)
}
