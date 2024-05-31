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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AndsReorderable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.SetExtractor
import org.scalatest.Inside

class SelectionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2
    with LogicalPlanningIntegrationTestSupport with Inside {

  /**
   * The assumptions on the ordering for these selectivities is based on that [[org.neo4j.cypher.internal.util.PredicateOrdering]] is used.
   */
  test("Should order predicates in selection first by cost then by selectivity") {
    val label = hasLabels("n", "Label")
    val otherLabel = hasLabels("n", "OtherLabel")
    val nProp = equals(prop("n", "prop"), literalInt(5))
    val nPropIn = in(prop("n", "prop"), listOfInt(5))
    val nParam = equals(v"n", parameter("param", CTAny))
    val nFooBar = equals(prop("n", "foo"), prop("n", "bar"))
    val selectivities = Map[Expression, Double](
      label -> 0.1, // More selective, so will be chosen for LabelScan.
      otherLabel -> 0.9, // So unselective it comes last, even if it has one store access less than nFooBar.
      nProp -> 0.2, // Most selective predicate with 1 store access.
      nPropIn -> 0.2, // -"-
      nParam -> 0.9, // Least selective, but cheapest, therefore comes first.
      nFooBar -> 0.1 // Very selective, but most costly with 2 store accesses. Comes almost last.
    )

    val plan = new givenConfig {
      // we have to force the planner not to do intersect scan
      cost = {
        case (Selection(_, _: IntersectionNodeByLabelsScan), _, _, _) => 50000.0
      }

      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) =>
          queryGraph.selections.predicates.foldLeft(1000.0) { case (rows, predicate) =>
            rows * selectivities(predicate.expr)
          }
      }
    }.getLogicalPlanFor(
      """MATCH (n:Label:OtherLabel)
        |WHERE n.prop = 5
        |AND n = $param
        |AND n.foo = n.bar
        |RETURN n""".stripMargin
    )._1
    val noArgs = Set.empty[LogicalVariable]
    plan should beLike {
      // We cannot use "plan should equal ..." because equality for [[Ands]] is overridden to not care about the order.
      // But unapply takes the order into account for [[Ands]].
      case Selection(
          Ands(SetExtractor(
            `nParam`,
            `nProp`,
            `nFooBar`,
            `otherLabel`
          )),
          NodeByLabelScan(LogicalVariable("n"), LabelName("Label"), `noArgs`, IndexOrderNone)
        ) => ()
    }
  }

  test("should pick more selective predicate first, even if it accesses a deeply nested property") {
    val nProp = greaterThan(prop("n", "prop"), literalInt(2))
    val nPropChain = equals(
      prop(prop(prop(prop(prop(prop(prop("n", "foo"), "bar"), "baz"), "blob"), "boing"), "peng"), "brrt"),
      literalInt(2)
    )

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .setLabelCardinality("M", 9)
      .build()
      .plan("MATCH (n:N) WHERE n.foo.bar.baz.blob.boing.peng.brrt = 2 AND n.prop > 2 RETURN n")

    val noArgs = Set.empty[LogicalVariable]

    plan.stripProduceResults should beLike {
      // We cannot use "plan should equal ..." because equality for [[Ands]] is overridden to not care about the order.
      // But unapply takes the order into account for [[Ands]].
      case Selection(
          Ands(SetExtractor(
            `nPropChain`, // more selective, but needs to access deeply nested property
            `nProp`
          )),
          NodeByLabelScan(LogicalVariable("n"), LabelName("N"), `noArgs`, IndexOrderNone)
        ) => ()
    }
  }

  test("should treat cached properties that read from store like normal properties") {
    val nPropFoo = equals(prop("n", "foo"), literalInt(2))
    val nPropBar = greaterThan(cachedNodePropFromStore("n", "bar"), literalInt(2))

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .build()
      .plan("MATCH (n:N) WHERE n.foo = 2 AND n.bar > 2 RETURN n.bar")

    val noArgs = Set.empty[LogicalVariable]

    plan.stripProduceResults should beLike {
      // We cannot use "plan should equal ..." because equality for [[Ands]] is overridden to not care about the order.
      // But unapply takes the order into account for [[Ands]].
      case Projection(
          Selection(
            Ands(SetExtractor(
              `nPropFoo`, // more selective
              `nPropBar` // is cached, but needs ro read from store
            )),
            NodeByLabelScan(LogicalVariable("n"), LabelName("N"), `noArgs`, IndexOrderNone)
          ),
          _
        ) => ()
    }
  }

  test("should not treat cached properties that do not read from store like normal properties") {
    val nPropFoo = equals(prop("nn", "foo"), literalInt(2))
    val nPropBar = greaterThan(cachedNodeProp("n", "bar", "nn"), literalInt(2))

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .build()
      .plan("MATCH (n:N) WHERE n.bar IS NOT NULL WITH n AS nn WHERE nn.foo = 2 AND nn.bar > 2 RETURN nn.bar")

    plan.stripProduceResults should beLike {
      // We cannot use "plan should equal ..." because equality for [[Ands]] is overridden to not care about the order.
      // But unapply takes the order into account for [[Ands]].
      case Projection(
          Selection(
            Ands(SetExtractor(
              `nPropBar`, // is cached and does not need to read from store
              `nPropFoo` // more selective
            )),
            _
          ),
          _
        ) => ()
    }
  }

  test("For multiple predicates accessing the same cached property for the first time: order by selectivity") {
    val nPropFoo1 = equals(cachedNodePropFromStore("n", "foo"), literalInt(2))
    val nPropFoo2 = greaterThan(cachedNodePropFromStore("n", "foo"), literalInt(1))

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .build()

    val plans = Seq(
      "MATCH (n:N) WHERE n.foo = 2 AND n.foo > 1 RETURN n.bar",
      "MATCH (n:N) WHERE n.foo > 1 AND n.foo = 2 RETURN n.bar"
    ).map(planner.plan(_)).map(_.stripProduceResults)

    val noArgs = Set.empty[LogicalVariable]

    plans.foreach { plan =>
      plan should beLike {
        // We cannot use "plan should equal ..." because equality for [[Ands]] is overridden to not care about the order.
        // But SetExtractor.unapply takes the order into account for [[Ands]].
        case Projection(
            Selection(
              Ands(SetExtractor(
                `nPropFoo1`, // more selective
                `nPropFoo2`
              )),
              NodeByLabelScan(LogicalVariable("n"), LabelName("N"), `noArgs`, IndexOrderNone)
            ),
            _
          ) => ()
      }
    }
  }

  test("should group inequality predicates with the same store access count") {
    val q =
      """MATCH (n)
        |WHERE n.prop1 > 123 AND n.prop2 < 321 AND n.prop3 >= 111 AND n.prop4 <= 333 AND
        |      n.otherProp < n.yetAnotherProp
        |RETURN n""".stripMargin

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .build()
      .plan(q)
      .stripProduceResults

    inside(plan) {
      case Selection(Ands(SetExtractor(AndsReorderable(singlePropPredicates), lastPredicate)), _) =>
        singlePropPredicates.size shouldBe 4
        lastPredicate shouldBe lessThan(
          prop("n", "otherProp"),
          prop("n", "yetAnotherProp")
        )
    }
  }

  test("Should solve extremely selective filter before any pattern predicates solved with SemiApply") {
    val planner = plannerBuilder()
      .removeNodeLookupIndex()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 1)
      .setLabelCardinality("B", 1)
      .setLabelCardinality("C", 1000)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("(:A)-[]->()", 500)
      .setRelationshipCardinality("(:B)-[]->()", 500)
      .setRelationshipCardinality("(:A)-[]->(:C)", 500)
      .setRelationshipCardinality("(:B)-[]->(:C)", 500)
      .setRelationshipCardinality("()-[]->(:C)", 1000)
      .setRelationshipCardinality("()-[]->()", 1000)
      .build()

    val query =
      """MATCH (a:A:B) WHERE
        |     EXISTS { (a)-[r]->(b:C) }
        | AND EXISTS { (a)-[q]->(c:C) }
        |RETURN a""".stripMargin

    planner.plan(query).stripProduceResults should equal(
      planner.subPlanBuilder()
        .semiApply()
        .|.filter("c:C")
        .|.expandAll("(a)-[q]->(c)")
        .|.argument("a")
        .semiApply()
        .|.filter("b:C")
        .|.expandAll("(a)-[r]->(b)")
        .|.argument("a")
        .filterExpression(andsReorderable("a:A", " a:B"))
        .allNodeScan("a")
        .build()
    )
  }

  test("Should group predicates when their respective costs are almost equal") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("Transaction", 10)
      .addNodeIndex("Transaction", List("date"), 0.000001, 1)
      .addNodeIndex("Transaction", List("amount"), 0.000002, 1)
      .addNodeIndex("Transaction", List("code"), 0.04999, 1)
      .build()

    val query =
      """MATCH (txn:Transaction)
        |WITH txn SKIP 0
        |  WHERE txn.date >= $from
        |  AND txn.date <= $to
        |  AND txn.amount >= $min
        |  AND txn.amount <= $max
        |  AND txn.currency = $currency
        |  AND txn.code IS NOT NULL
        |RETURN txn""".stripMargin

    planner.plan(query).stripProduceResults should equal(
      planner.subPlanBuilder()
        .filterExpressionOrString(
          andsReorderable(
            "cacheNFromStore[txn.date] >= $from",
            "cacheNFromStore[txn.date] <= $to",
            "cacheNFromStore[txn.amount] >= $min",
            "cacheNFromStore[txn.amount] <= $max"
          ),
          "txn.code IS NOT NULL",
          "txn.currency = $currency"
        )
        .skip(0)
        .nodeByLabelScan("txn", "Transaction", IndexOrderNone)
        .build()
    )
  }

  test("Should not group predicates with different estimated selectivities") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .build()

    val query =
      """WITH $param AS x SKIP 0
        |MATCH (n)
        |WHERE n.prop CONTAINS x AND n.otherProp IS NOT NULL
        |RETURN x""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .filter("n.prop CONTAINS x", "n.otherProp IS NOT NULL")
      .apply()
      .|.allNodeScan("n", "x")
      .projection("$param AS x")
      .skip(0)
      .argument()
      .build()
  }
}
