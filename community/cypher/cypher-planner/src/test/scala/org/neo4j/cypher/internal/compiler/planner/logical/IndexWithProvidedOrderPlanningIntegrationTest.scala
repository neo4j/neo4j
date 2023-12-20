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
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverSetup
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithGreedyConnectComponents
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithIDPConnectComponents
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.BOTH
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.NONE
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class IndexWithProvidedOrderIDPPlanningIntegrationTest
    extends IndexWithProvidedOrderPlanningIntegrationTest(QueryGraphSolverWithIDPConnectComponents)

class IndexWithProvidedOrderGreedyPlanningIntegrationTest
    extends IndexWithProvidedOrderPlanningIntegrationTest(QueryGraphSolverWithGreedyConnectComponents)

abstract class IndexWithProvidedOrderPlanningIntegrationTest(queryGraphSolverSetup: QueryGraphSolverSetup)
    extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with LogicalPlanningTestSupport2
    with PlanMatchHelp {

  locally {
    queryGraphSolver = queryGraphSolverSetup.queryGraphSolver()
  }

  override def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .enableConnectComponentsPlanner(queryGraphSolverSetup.useIdpConnectComponents)
      .enablePlanningIntersectionScans()

  case class TestOrder(
    indexOrder: IndexOrder,
    cypherToken: String,
    indexOrderCapability: IndexOrderCapability,
    sortOrder: String => ColumnOrder
  )
  private val ASCENDING_BOTH = TestOrder(IndexOrderAscending, "ASC", BOTH, c => Ascending(varFor(c)))
  private val DESCENDING_BOTH = TestOrder(IndexOrderDescending, "DESC", BOTH, c => Descending(varFor(c)))

  override val pushdownPropertyReads: Boolean = false

  for (TestOrder(plannedOrder, cypherToken, orderCapability, sortOrder) <- List(ASCENDING_BOTH, DESCENDING_BOTH)) {

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._1 should equal(
        Projection(
          nodeIndexSeek("n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
          Map(v"n.prop" -> prop("n", "prop"))
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order, even after initial WITH"
    ) {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        }.getLogicalPlanFor(
          s"WITH 1 AS foo MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop AS p ORDER BY n.prop $cypherToken",
          stripProduceResults = false
        )

      plan._1 should equal(
        new LogicalPlanBuilder()
          .produceResults("p")
          .projection("n.prop AS p")
          .projection("1 AS foo")
          .nodeIndexOperator("n:Awesome(prop > 'foo')", indexOrder = plannedOrder, indexType = IndexType.RANGE)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order, even after initial WITH that introduces variable used in subsequent WHERE"
    ) {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setLabelCardinality("Awesome", 10)
        .addNodeIndex("Awesome", Seq("prop"), 1.0, 0.1, providesOrder = orderCapability)
        .build()

      val plan = planner.plan(
        s"""
           |WITH $$param AS x
           |MATCH (n:Awesome)
           |WHERE n.prop > x
           |RETURN n.prop AS prop
           |ORDER BY n.prop $cypherToken""".stripMargin
      ).stripProduceResults

      plan should equal(
        planner.subPlanBuilder()
          .projection("n.prop AS prop")
          .apply()
          .|.nodeIndexOperator("n:Awesome(prop > x)", indexOrder = plannedOrder, argumentIds = Set("x"))
          .projection("$param AS x")
          .argument()
          .build()
      )
    }

    test(s"$cypherToken-$orderCapability: Order by variable from label scan should plan with provided order") {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setLabelCardinality("Awesome", 10)
        .build()

      val plan = planner.plan(
        s"MATCH (n:Awesome) RETURN n ORDER BY n $cypherToken"
      )

      plan should equal(
        planner.planBuilder()
          .produceResults("n")
          .nodeByLabelScan("n", "Awesome", plannedOrder)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable from label scan with token index with no ordering should not plan with provided order"
    ) {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setLabelCardinality("Awesome", 10)
        .addNodeLookupIndex(IndexOrderCapability.NONE)
        .build()

      val plan = planner.plan(
        s"MATCH (n:Awesome) RETURN n ORDER BY n $cypherToken"
      )

      plan should equal(
        planner.planBuilder()
          .produceResults("n")
          .sortColumns(Seq(sortOrder("n")))
          .nodeByLabelScan("n", "Awesome")
          .build()
      )
    }

    test(s"$cypherToken-$orderCapability: Order by variable from union label scan should plan with provided order") {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setLabelCardinality("A", 10)
        .setLabelCardinality("B", 10)
        .build()

      val plan = planner.plan(
        s"MATCH (n:A|B) RETURN n ORDER BY n $cypherToken"
      )

      plan should equal(
        planner.planBuilder()
          .produceResults("n")
          .unionNodeByLabelsScan("n", Seq("A", "B"), plannedOrder)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable with label disjunction with token index with no ordering should not plan unionNodeByLabelsScan"
    ) {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setLabelCardinality("A", 10)
        .setLabelCardinality("B", 10)
        .addNodeLookupIndex(IndexOrderCapability.NONE)
        .build()

      val plan = planner.plan(
        s"MATCH (n:A|B) RETURN n ORDER BY n $cypherToken"
      )

      plan should equal(
        planner.planBuilder()
          .produceResults("n")
          .sortColumns(Seq(sortOrder("n")))
          .distinct("n AS n")
          .union()
          .|.nodeByLabelScan("n", "B", IndexOrderNone)
          .nodeByLabelScan("n", "A", IndexOrderNone)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable from intersection label scan should plan with provided order"
    ) {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setLabelCardinality("A", 10)
        .setLabelCardinality("B", 10)
        .build()

      val plan = planner.plan(
        s"MATCH (n:A&B) RETURN n ORDER BY n $cypherToken"
      )

      plan should equal(
        planner.planBuilder()
          .produceResults("n")
          .intersectionNodeByLabelsScan("n", Seq("A", "B"), plannedOrder)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable with label conjunction with token index with no ordering should not plan intersectionNodeByLabelsScan"
    ) {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setLabelCardinality("A", 10)
        .setLabelCardinality("B", 10)
        .addNodeLookupIndex(IndexOrderCapability.NONE)
        .build()

      val plan = planner.plan(
        s"MATCH (n:A&B) RETURN n ORDER BY n $cypherToken"
      )

      plan should equal(
        planner.planBuilder()
          .produceResults("n")
          .sortColumns(Seq(sortOrder("n")))
          .filter("n:B")
          .nodeByLabelScan("n", "A", IndexOrderNone)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable from relationship type scan should plan with provided order"
    ) {
      val query = s"MATCH (n)-[r:REL]->(m) RETURN r ORDER BY r $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(1000)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .build()

      val plan = planner.plan(query)

      plan should equal(planner.subPlanBuilder()
        .produceResults("r")
        .relationshipTypeScan("(n)-[r:REL]->(m)", plannedOrder)
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable from relationship type scan with token index with no ordering should not plan with provided order"
    ) {
      val query = s"MATCH (n)-[r:REL]->(m) RETURN r ORDER BY r $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(1000)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .addRelationshipLookupIndex(IndexOrderCapability.NONE)
        .build()

      val plan = planner.plan(query)

      plan should equal(planner.subPlanBuilder()
        .produceResults("r")
        .sortColumns(Seq(sortOrder("r")))
        .relationshipTypeScan("(n)-[r:REL]->(m)")
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable from union relationship type scan should plan with provided order"
    ) {
      val query = s"MATCH (n)-[r:REL|LER]->(m) RETURN r ORDER BY r $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(1000)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .setRelationshipCardinality("()-[:LER]-()", 100)
        .build()

      val plan = planner.plan(query)

      plan should equal(planner.subPlanBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(n)-[r:REL|LER]->(m)", plannedOrder)
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable with reltype disjunction with token index with no ordering should not plan unionRelationshipTypesScan"
    ) {
      val query = s"MATCH (n)-[r:REL|LER]->(m) RETURN r ORDER BY r $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(1000)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .setRelationshipCardinality("()-[:LER]-()", 100)
        .addRelationshipLookupIndex(IndexOrderCapability.NONE)
        .build()

      val plan = planner.plan(query)

      plan should equal(planner.subPlanBuilder()
        .produceResults("r")
        .sortColumns(Seq(sortOrder("r")))
        .distinct("r AS r", "n AS n", "m AS m")
        .union()
        .|.relationshipTypeScan("(n)-[r:LER]->(m)")
        .relationshipTypeScan("(n)-[r:REL]->(m)")
        .build())
    }

    test(s"$cypherToken-$orderCapability: Order by id of variable from label scan should plan with provided order") {
      val query = s"MATCH (n:L) RETURN n ORDER BY id(n) $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("L", 100)
        .build()

      val plan = planner.plan(query)

      plan should equal(planner.subPlanBuilder()
        .produceResults("n")
        .nodeByLabelScan("n", "L", plannedOrder)
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by id of variable from relationship type scan should plan with provided order"
    ) {
      val query = s"MATCH (n)-[r:REL]->(m) RETURN r ORDER BY id(r) $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(1000)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .build()

      val plan = planner.plan(query)

      plan should equal(planner.subPlanBuilder()
        .produceResults("r")
        .relationshipTypeScan("(n)-[r:REL]->(m)", plannedOrder)
        .build())
    }

    test(s"$cypherToken-$orderCapability: Order by variable from label and join in MATCH with multiple labels") {
      val plan = new givenConfig {
        indexOn("Foo", "prop")
        indexOn("Bar", "unused") // otherwise we don't get a label id for Bar
        labelCardinality = Map(
          "Foo" -> 10.0,
          "Bar" -> 1000.0
        )
        cost = {
          // Selection is usually cheap. Let's avoid it by making it expensive to plan, so that our options are index seeks and label scans.
          case (Selection(_, _: NodeIndexSeek), _, _, _)                => 50000.0
          case (Selection(_, _: NodeIndexScan), _, _, _)                => 50000.0
          case (Selection(_, _: NodeByLabelScan), _, _, _)              => 50000.0
          case (Selection(_, _: IntersectionNodeByLabelsScan), _, _, _) => 50000.0
        }
      }.getLogicalPlanFor(
        s"MATCH (n:Foo:Bar) WHERE n.prop > 0 RETURN n ORDER BY n $cypherToken",
        stripProduceResults = false
      )

      // This test does not show we pick this plan under normal conditions. We don't.
      // Instead, it shows, that the correct index order can get injected into a LabelScan produced by [[selectHasLabelWithJoin]].
      plan._1 should equal(
        new LogicalPlanBuilder()
          .produceResults("n")
          .nodeHashJoin("n")
          .|.nodeByLabelScan("n", "Bar", plannedOrder)
          .nodeIndexOperator("n:Foo(prop > 0)", indexType = IndexType.RANGE)
          .build()
      )
    }

    test(s"$cypherToken-$orderCapability: Should not order label scan if ORDER BY aggregation of that node") {
      val plan = new givenConfig().getLogicalPlanFor(
        s"MATCH (n:Awesome)-[r]-(m) RETURN m AS mm, count(n) AS c ORDER BY c $cypherToken",
        stripProduceResults = false
      )

      plan._1 should equal(
        new LogicalPlanBuilder()
          .produceResults("mm", "c")
          .sortColumns(Seq(sortOrder("c")))
          .aggregation(Seq("m AS mm"), Seq("count(n) AS c"))
          .expandAll("(n)-[r]-(m)")
          .nodeByLabelScan("n", "Awesome", IndexOrderNone)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable renamed in WITH from label scan should plan with provided order"
    ) {
      val plan = new givenConfig().getLogicalPlanFor(
        s"""MATCH (n:Awesome)
           |WITH n AS nnn
           |MATCH (m)-[r]->(nnn)
           |RETURN nnn ORDER BY nnn $cypherToken""".stripMargin,
        stripProduceResults = false
      )

      plan._1 should equal(
        new LogicalPlanBuilder()
          .produceResults("nnn")
          .expandAll("(nnn)<-[r]-(m)")
          .projection("n AS nnn")
          .nodeByLabelScan("n", "Awesome", plannedOrder)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by variable from label scan should plan with provided order and PartialSort"
    ) {
      val plan = new givenConfig().getLogicalPlanFor(
        s"MATCH (n:Awesome) WITH n, n.foo AS foo RETURN n ORDER BY n $cypherToken, foo",
        stripProduceResults = false
      )

      plan._1 should equal(
        new LogicalPlanBuilder()
          .produceResults("n")
          .partialSortColumns(Seq(sortOrder("n")), Seq(Ascending(v"foo")))
          .projection("n.foo AS foo")
          .nodeByLabelScan("n", "Awesome", plannedOrder)
          .build()
      )
    }

    test(s"$cypherToken-$orderCapability: Should not need to sort after Label disjunction") {
      val query = s"MATCH (m) WHERE m:A OR m:B RETURN m ORDER BY m $cypherToken"

      val plan = plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("A", 60)
        .setLabelCardinality("B", 60)
        .build()
        .plan(query)
        .stripProduceResults

      plan should (equal(new LogicalPlanBuilder(wholePlan = false)
        .unionNodeByLabelsScan("m", Seq("A", "B"), plannedOrder)
        .build())
        or equal(new LogicalPlanBuilder(wholePlan = false)
          .unionNodeByLabelsScan("m", Seq("B", "A"), plannedOrder)
          .build()))
    }

    test(s"$cypherToken-$orderCapability: Should do a PartialSort after ordered union for Label disjunction") {
      val query = s"MATCH (m) WHERE m:A OR m:B RETURN m ORDER BY m $cypherToken, m.prop"

      val plan = plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("A", 60)
        .setLabelCardinality("B", 60)
        .build()
        .plan(query)
        .stripProduceResults

      plan should (equal(new LogicalPlanBuilder(wholePlan = false)
        .partialSortColumns(Seq(sortOrder("m")), Seq(Ascending(v"m.prop")))
        .projection("m.prop AS `m.prop`")
        .unionNodeByLabelsScan("m", Seq("A", "B"), plannedOrder)
        .build())
        or equal(new LogicalPlanBuilder(wholePlan = false)
          .partialSortColumns(Seq(sortOrder("m")), Seq(Ascending(v"m.prop")))
          .projection("m.prop AS `m.prop`")
          .unionNodeByLabelsScan("m", Seq("B", "A"), plannedOrder)
          .build()))
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property should plan sort if index does not provide order"
    ) {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop")
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._1 should equal(
        Sort(
          Projection(
            nodeIndexSeek(
              "n:Awesome(prop > 'foo')",
              indexOrder =
                IndexOrderNone
            ),
            Map(v"n.prop" -> prop("n", "prop"))
          ),
          Seq(sortOrder("n.prop"))
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order, even after initial WITH and with Expand"
    ) {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        }.getLogicalPlanFor(
          s"WITH 1 AS foo MATCH (n:Awesome)-[r]->(m) WHERE n.prop > 'foo' RETURN n.prop AS p ORDER BY n.prop $cypherToken",
          stripProduceResults = false
        )

      plan._1 should equal(
        new LogicalPlanBuilder()
          .produceResults("p")
          .projection("n.prop AS p")
          .projection("1 AS foo")
          .expandAll("(n)-[r]->(m)")
          .nodeIndexOperator("n:Awesome(prop > 'foo')", indexOrder = plannedOrder, indexType = IndexType.RANGE)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property should plan partial sort if index does partially provide order"
    ) {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken, n.foo ASC"

      plan._1 should equal(
        PartialSort(
          Projection(
            Projection(
              nodeIndexSeek(
                "n:Awesome(prop > 'foo')",
                indexOrder = plannedOrder
              ),
              Map(v"n.prop" -> prop("n", "prop"))
            ),
            Map(v"n.foo" -> prop("n", "foo"))
          ),
          Seq(sortOrder("n.prop")),
          Seq(Ascending(v"n.foo"))
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (directed) should plan partial sort if index does partially provide order"
    ) {
      val query =
        s"MATCH (a)-[r:REL]->(b) WHERE r.prop IS NOT NULL RETURN r.prop ORDER BY r.prop $cypherToken, r.foo ASC"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(10)
        .setRelationshipCardinality("()-[:REL]-()", 10)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .partialSortColumns(Seq(sortOrder("r.prop")), Seq(Ascending(v"r.foo")))
        .projection("r.foo AS `r.foo`")
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]->(b)",
          indexOrder = plannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (directed) should plan partial sort if index seek does partially provide order"
    ) {
      val query = s"MATCH (a)-[r:REL]->(b) WHERE r.prop > 123 RETURN r.prop ORDER BY r.prop $cypherToken, r.foo ASC"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(10)
        .setRelationshipCardinality("()-[:REL]-()", 10)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .partialSortColumns(Seq(sortOrder("r.prop")), Seq(Ascending(v"r.foo")))
        .projection("r.foo AS `r.foo`")
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop > 123)]->(b)",
          indexOrder = plannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property should plan partial sort if index does partially provide order and the second column is more complicated"
    ) {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken, n.foo + 1 ASC"

      plan._1 should equal(new LogicalPlanBuilder(wholePlan = false)
        .partialSortColumns(Seq(sortOrder("n.prop")), Seq(Ascending(v"n.foo + 1")))
        .projection("n.foo + 1 AS `n.foo + 1`")
        .projection("n.prop AS `n.prop`")
        .nodeIndexOperator("n:Awesome(prop > 'foo')", indexOrder = plannedOrder, getValue = _ => DoNotGetValue)
        .build())
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan multiple partial sorts") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' WITH n, n.prop AS p, n.foo AS f ORDER BY p $cypherToken, f ASC RETURN p ORDER BY p $cypherToken, f ASC, n.bar ASC"

      plan._1 should equal(
        PartialSort(
          Projection(
            PartialSort(
              Projection(
                nodeIndexSeek(
                  "n:Awesome(prop > 'foo')",
                  indexOrder = plannedOrder
                ),
                Map(v"f" -> prop("n", "foo"), v"p" -> prop("n", "prop"))
              ),
              Seq(sortOrder("p")),
              Seq(Ascending(v"f"))
            ),
            Map(v"n.bar" -> prop("n", "bar"))
          ),
          Seq(sortOrder("p"), Ascending(v"f")),
          Seq(Ascending(v"n.bar"))
        )
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property renamed in an earlier WITH") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor
          s"""MATCH (n:Awesome) WHERE n.prop > 'foo'
             |WITH n AS nnn
             |MATCH (m)-[r]->(nnn)
             |RETURN nnn.prop ORDER BY nnn.prop $cypherToken""".stripMargin

      plan._1 should equal(
        Projection(
          Expand(
            Projection(
              nodeIndexSeek(
                "n:Awesome(prop > 'foo')",
                indexOrder = plannedOrder
              ),
              Map(v"nnn" -> v"n")
            ),
            v"nnn",
            SemanticDirection.INCOMING,
            Seq.empty,
            v"m",
            v"r"
          ),
          Map(v"nnn.prop" -> prop("nnn", "prop"))
        )
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property renamed in same return") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor
          s"""MATCH (n:Awesome) WHERE n.prop > 'foo'
             |RETURN n AS m ORDER BY m.prop $cypherToken""".stripMargin

      plan._1 should equal(
        Projection(
          nodeIndexSeek("n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
          Map(v"m" -> v"n")
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Cannot order by index when ordering is on same property name, but different node"
    ) {
      assume(
        queryGraphSolverSetup == QueryGraphSolverWithIDPConnectComponents,
        "This test requires the IDP connect components planner"
      )

      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        }.getLogicalPlanFor(
          s"MATCH (m:Awesome), (n:Awesome) WHERE n.prop > 'foo' RETURN m.prop ORDER BY m.prop $cypherToken",
          stripProduceResults = false
        )

      val so = sortOrder("m.prop")
      withClue(plan._1) {
        plan._1.folder.treeCount {
          case Sort(_, Seq(`so`)) => ()
        } shouldBe 1
      }
    }

    test(
      s"$cypherToken-$orderCapability: Cannot order by index when ordering is on same property name, but different node with relationship"
    ) {
      assume(
        queryGraphSolverSetup == QueryGraphSolverWithIDPConnectComponents,
        "This test requires the IDP connect components planner"
      )

      // With two relationships we use IDP to get the best plan.
      // By keeping the best overall and the best sorted plan, we should only have one sort.
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        }.getLogicalPlanFor(
          s"MATCH (m:Awesome)-[r]-(x)-[p]-(y), (n:Awesome) WHERE n.prop > 'foo' RETURN m.prop ORDER BY m.prop $cypherToken",
          stripProduceResults = false
        )

      val so = sortOrder("m.prop")
      withClue(plan._1) {
        plan._1.folder.treeCount {
          case Sort(_, Seq(`so`)) => ()
        } shouldBe 1
      }
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (starts with scan)"
    ) {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._1 should equal(
        Projection(
          nodeIndexSeek(
            "n:Awesome(prop STARTS WITH 'foo')",
            indexOrder = plannedOrder
          ),
          Map(v"n.prop" -> prop("n", "prop"))
        )
      )
    }

    for (predicate <- Seq("CONTAINS", "ENDS WITH")) {
      // This is supported because internally all kernel indexes which support ordering will just scan and filter to serve contains
      test(
        s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order ($predicate scan)"
      ) {
        val planner = plannerBuilder()
          .addNodeIndex("Awesome", Seq("prop"), 0.01, 0.0001, providesOrder = orderCapability)
          .setAllNodesCardinality(100)
          .setLabelCardinality("Awesome", 10)
          .build()

        val plan =
          planner.plan(s"MATCH (n:Awesome) WHERE n.prop $predicate 'foo' RETURN n.prop ORDER BY n.prop $cypherToken")

        plan should equal(
          planner.planBuilder()
            .produceResults("`n.prop`")
            .projection("cacheN[n.prop] AS `n.prop`")
            .filter(s"cacheNFromStore[n.prop] $predicate 'foo'")
            .nodeIndexOperator("n:Awesome(prop)", indexOrder = plannedOrder, indexType = IndexType.RANGE)
            .build()
        )
      }
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (scan)") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop IS NOT NULL RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._1 should equal(
        Projection(
          nodeIndexSeek(
            "n:Awesome(prop)",
            indexOrder = plannedOrder
          ),
          Map(v"n.prop" -> prop("n", "prop"))
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (undirected) should plan with provided order (scan)"
    ) {
      val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r.prop ORDER BY r.prop $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]-(b)",
          indexOrder = plannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (undirected) should plan with provided order (scan) with NOT IS NULL"
    ) {
      val query = s"MATCH (a)-[r:REL]-(b) WHERE NOT(r.prop IS NULL) RETURN r.prop ORDER BY r.prop $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]-(b)",
          indexOrder = plannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (undirected) should plan with provided order (seek)"
    ) {
      val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop > 123 RETURN r.prop ORDER BY r.prop $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop > 123)]-(b)",
          indexOrder = plannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (undirected) should plan with provided order (contains scan)"
    ) {
      val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop CONTAINS 'sub' RETURN r.prop ORDER BY r.prop $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .addRelationshipIndex(
          "REL",
          Seq("prop"),
          1.0,
          0.01,
          withValues = true,
          providesOrder = orderCapability,
          indexType = IndexType.TEXT
        )
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop CONTAINS 'sub')]-(b)",
          indexOrder = plannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.TEXT
        )
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (undirected) should plan with provided order (ends with scan)"
    ) {
      val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop ENDS WITH 'sub' RETURN r.prop ORDER BY r.prop $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .addRelationshipIndex(
          "REL",
          Seq("prop"),
          1.0,
          0.01,
          withValues = true,
          providesOrder = orderCapability,
          indexType = IndexType.TEXT
        )
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop ENDS WITH 'sub')]-(b)",
          indexOrder = plannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.TEXT
        )
        .build())
    }

    /**
     * TODO replace
     *
     * Returns cardinalities based on matching against given patternNodes + predicates.
     * Only matches if the patternNodes and all predicates are covered by the patternNodesPredicateSelectivity nested map.
     * The resulting cardinality is the product of baseCardinality and the given selectivity for all matching predicates.
     */
    def byPredicateSelectivity(
      baseCardinality: Double,
      patternNodesPredicateSelectivity: Map[Set[LogicalVariable], Map[Expression, Double]]
    ): PartialFunction[PlannerQuery, Double] = {
      def mapsPatternNodes(patternNodes: Set[LogicalVariable]) = patternNodesPredicateSelectivity.contains(patternNodes)
      def mapsPredicate(patternNodes: Set[LogicalVariable], predicate: Predicate) =
        patternNodesPredicateSelectivity(patternNodes).contains(predicate.expr)

      {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _)
          if mapsPatternNodes(queryGraph.patternNodes) && queryGraph.selections.predicates.forall(mapsPredicate(
            queryGraph.patternNodes,
            _
          )) =>
          val selectivityMap = patternNodesPredicateSelectivity(queryGraph.patternNodes)
          queryGraph.selections.predicates.map(_.expr).map(selectivityMap).product * baseCardinality
      }
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an Apply") {
      val plan =
        new givenConfig {
          indexOn("A", "prop").providesOrder(orderCapability)
          indexOn("B", "prop").providesOrder(orderCapability)
          cardinality = mapCardinality(byPredicateSelectivity(
            10000,
            Map(
              Set[LogicalVariable](v"a") -> Map(
                // 50% have :A
                hasLabels("a", "A") -> 0.5,
                // 10% have .prop > 'foo'
                AndedPropertyInequalities(
                  v"a",
                  prop("a", "prop"),
                  NonEmptyList(greaterThan(prop("a", "prop"), literalString("foo")))
                ) -> 0.1,
                // 1% have .prop = <given value>
                Equals(prop("a", "prop"), prop("b", "prop"))(pos) -> 0.01
              ),
              Set[LogicalVariable](v"b") -> Map(
                // 50% have :B
                hasLabels("b", "B") -> 0.5,
                // 0.01% have .prop = <given value>
                Equals(prop("a", "prop"), prop("b", "prop"))(pos) -> 0.0001
              )
            )
          ))
        } getLogicalPlanFor s"MATCH (a:A), (b:B) WHERE a.prop > 'foo' AND a.prop = b.prop RETURN a.prop ORDER BY a.prop $cypherToken"

      plan._1 should equal(
        Projection(
          Apply(
            nodeIndexSeek(
              "a:A(prop > 'foo')",
              indexOrder = plannedOrder
            ),
            nodeIndexSeek(
              "b:B(prop = ???)",
              paramExpr = Some(cachedNodePropFromStore("a", "prop")),
              labelId = 1,
              argumentIds = Set("a")
            )
          ),
          Map(v"a.prop" -> cachedNodeProp("a", "prop"))
        )
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed relationship property in a plan with an Apply") {
      val query =
        s"MATCH (a)-[r:REL]-(b), (c)-[r2:REL2]-(d) WHERE r.prop > 'foo' AND r.prop = r2.prop RETURN r.prop ORDER BY r.prop $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 50)
        .setRelationshipCardinality("()-[:REL2]-()", 50)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.1, withValues = true, providesOrder = orderCapability)
        .addRelationshipIndex("REL2", Seq("prop"), 1.0, 0.02, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .apply()
        .|.relationshipIndexOperator(
          "(c)-[r2:REL2(prop = ???)]-(d)",
          paramExpr = Some(cachedRelProp("r", "prop")),
          argumentIds = Set("a", "r", "b"),
          indexType = IndexType.RANGE
        )
        .relationshipIndexOperator(
          "(a)-[r:REL(prop > 'foo')]-(b)",
          indexOrder = plannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(s"$cypherToken-$orderCapability: Order by label variable in a plan with an Apply") {
      val plan = new givenConfig {
        indexOn("B", "prop")
      }.getLogicalPlanFor(
        s"MATCH (a:A), (b:B) WHERE a.prop = b.prop RETURN a ORDER BY a $cypherToken",
        stripProduceResults = false
      )

      plan._1 should equal(
        new LogicalPlanBuilder()
          .produceResults("a")
          .apply()
          .|.nodeIndexOperator(
            "b:B(prop = ???)",
            paramExpr = Some(prop("a", "prop")),
            argumentIds = Set("a"),
            indexType = IndexType.RANGE
          )
          .nodeByLabelScan("a", "A", plannedOrder)
          .build()
      )
    }

    test(s"$cypherToken-$orderCapability: Order by relationship variable in a plan with an Apply") {
      val query = s"MATCH (a)-[r:REL]-(b), (c)-[r2:REL2]-(d) WHERE r2.prop = r.prop RETURN r ORDER BY r $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(10000)
        .setAllRelationshipsCardinality(1000)
        .setRelationshipCardinality("()-[:REL]-()", 50)
        .setRelationshipCardinality("()-[:REL2]-()", 500)
        .addRelationshipIndex("REL2", Seq("prop"), 0.1, 0.002)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(
        planner.subPlanBuilder()
          .apply()
          .|.relationshipIndexOperator(
            "(c)-[r2:REL2(prop = ???)]-(d)",
            paramExpr = Some(prop("r", "prop")),
            argumentIds = Set("a", "r", "b"),
            indexType = IndexType.RANGE
          )
          .relationshipTypeScan("(a)-[r:REL]-(b)", plannedOrder)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by label variable in a plan where NIJ is not possible, with 1 relationship pattern on the RHS"
    ) {
      val planner = plannerBuilder()
        .setAllNodesCardinality(10000)
        .setLabelCardinality("A", 1)
        .setLabelCardinality("B", 100)
        .setRelationshipCardinality("()-[]->()", 50000)
        .setRelationshipCardinality("(:B)-[]->()", 50)
        .addNodeIndex("B", Seq("prop"), 0.001, 0.001, providesOrder = orderCapability)
        .build()

      planner.plan(
        s"MATCH (a:A), (b:B)-[r]->(c) WHERE a.prop > b.prop - c.prop RETURN a ORDER BY a $cypherToken"
      ) should equal(
        planner.planBuilder()
          .produceResults("a")
          .filter("cacheN[a.prop] > b.prop - c.prop")
          .cartesianProduct()
          .|.expandAll("(b)-[r]->(c)")
          .|.nodeByLabelScan("b", "B", IndexOrderNone)
          .cacheProperties("cacheNFromStore[a.prop]")
          .nodeByLabelScan("a", "A", plannedOrder)
          .build()
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed properties in a plan with an Apply needs Partial Sort if RHS order required"
    ) {
      val plan =
        new givenConfig {
          indexOn("A", "prop").providesOrder(orderCapability)
          indexOn("B", "prop").providesOrder(orderCapability)
          // This query is very fragile in the sense that the slightest modification will result in a stupid plan
        } getLogicalPlanFor s"MATCH (a:A), (b:B) WHERE a.prop STARTS WITH 'foo' AND b.prop > a.prop RETURN a.prop, b.prop ORDER BY a.prop $cypherToken, b.prop $cypherToken"

      plan._1 should equal(
        PartialSort(
          Projection(
            Apply(
              nodeIndexSeek("a:A(prop STARTS WITH 'foo')", indexOrder = plannedOrder),
              nodeIndexSeek(
                "b:B(prop > ???)",
                paramExpr = Some(cachedNodePropFromStore("a", "prop")),
                labelId = 1,
                argumentIds = Set("a"),
                indexOrder = plannedOrder
              )
            ),
            Map(v"a.prop" -> cachedNodeProp("a", "prop"), v"b.prop" -> prop("b", "prop"))
          ),
          Seq(sortOrder("a.prop")),
          Seq(sortOrder("b.prop"))
        )
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an renaming Projection") {
      val plan =
        new givenConfig {
          indexOn("A", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor s"MATCH (a:A) WHERE a.prop > 'foo' WITH a.prop AS theProp, 1 AS x RETURN theProp ORDER BY theProp $cypherToken"

      plan._1 should equal(
        Projection(
          nodeIndexSeek(
            "a:A(prop > 'foo')",
            indexOrder = plannedOrder
          ),
          Map(v"theProp" -> prop("a", "prop"), v"x" -> literalInt(1))
        )
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an aggregation and an expand") {
      val plan =
        new givenConfig {
          indexOn("A", "prop").providesOrder(orderCapability)
          cardinality = mapCardinality {
            // Force the planner to start at a
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a") => 100.0
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b") => 2000.0
          }
        } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN a.prop, count(b) ORDER BY a.prop $cypherToken"

      plan._1 should equal(
        OrderedAggregation(
          Expand(
            nodeIndexSeek(
              "a:A(prop > 'foo')",
              indexOrder = plannedOrder
            ),
            v"a",
            SemanticDirection.OUTGOING,
            Seq.empty,
            v"b",
            v"r"
          ),
          Map(v"a.prop" -> cachedNodePropFromStore("a", "prop")),
          Map(v"count(b)" -> count(v"b")),
          Seq(cachedNodePropFromStore("a", "prop"))
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property in a plan with partial provided order and with an expand"
    ) {
      val plan =
        new givenConfig {
          indexOn("A", "prop").providesOrder(orderCapability)
          cardinality = mapCardinality {
            // Force the planner to start at a
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a") => 100.0
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b") => 2000.0
          }
        } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop $cypherToken, b.prop"

      plan._1 should equal(
        PartialSort(
          Projection(
            Projection(
              Expand(
                nodeIndexSeek(
                  "a:A(prop > 'foo')",
                  indexOrder = plannedOrder
                ),
                v"a",
                SemanticDirection.OUTGOING,
                Seq.empty,
                v"b",
                v"r"
              ),
              Map(v"a.prop" -> prop("a", "prop"))
            ),
            Map(v"b.prop" -> prop("b", "prop"))
          ),
          Seq(sortOrder("a.prop")),
          Seq(Ascending(v"b.prop"))
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property in a plan with partial provided order and with two expand - should plan partial sort in the middle"
    ) {
      val plan =
        new givenConfig {
          indexOn("A", "prop").providesOrder(orderCapability)
          cardinality = mapCardinality {
            // Force the planner to start at a
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a") => 1.0
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b") => 2000.0
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"c") => 2000.0
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b") => 50.0
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b", v"c") =>
              500.0
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _)
              if queryGraph.patternNodes == Set(v"a", v"b", v"c") =>
              1000.0
          }
        } getLogicalPlanFor s"MATCH (a:A)-[r]->(b)-[q]->(c) WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop $cypherToken, b.prop"

      plan._1 should equal(
        Selection(
          Seq(not(equals(v"q", v"r"))),
          Expand(
            PartialSort(
              Projection(
                Projection(
                  Expand(
                    nodeIndexSeek(
                      "a:A(prop > 'foo')",
                      indexOrder = plannedOrder
                    ),
                    v"a",
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    v"b",
                    v"r"
                  ),
                  Map(v"a.prop" -> prop("a", "prop"))
                ),
                Map(v"b.prop" -> prop("b", "prop"))
              ),
              Seq(sortOrder("a.prop")),
              Seq(Ascending(v"b.prop"))
            ),
            v"b",
            SemanticDirection.OUTGOING,
            Seq.empty,
            v"c",
            v"q"
          )
        )
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with a distinct") {
      val plan =
        new givenConfig {
          indexOn("A", "prop").providesOrder(orderCapability)
          cardinality = mapCardinality {
            // Force the planner to start at a
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a") => 100.0
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b") => 2000.0
          }
        } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN DISTINCT a.prop ORDER BY a.prop $cypherToken"

      plan._1 should equal(
        OrderedDistinct(
          Expand(
            nodeIndexSeek(
              "a:A(prop > 'foo')",
              indexOrder = plannedOrder
            ),
            v"a",
            SemanticDirection.OUTGOING,
            Seq.empty,
            v"b",
            v"r"
          ),
          Map(v"a.prop" -> cachedNodePropFromStore("a", "prop")),
          Seq(cachedNodePropFromStore("a", "prop"))
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (directed) in a plan with a distinct"
    ) {
      val query = s"MATCH (a)<-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN DISTINCT r.prop ORDER BY r.prop $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(10)
        .setRelationshipCardinality("()-[:REL]-()", 10)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .orderedDistinct(Seq("cacheRFromStore[r.prop]"), "cacheRFromStore[r.prop] AS `r.prop`")
        .relationshipIndexOperator("(a)<-[r:REL(prop)]-(b)", indexOrder = plannedOrder, indexType = IndexType.RANGE)
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (directed) in a plan with a distinct (seek)"
    ) {
      val query = s"MATCH (a)<-[r:REL]-(b) WHERE r.prop > 123 RETURN DISTINCT r.prop ORDER BY r.prop $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(10)
        .setRelationshipCardinality("()-[:REL]-()", 10)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .orderedDistinct(Seq("cacheRFromStore[r.prop]"), "cacheRFromStore[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)<-[r:REL(prop > 123)]-(b)",
          indexOrder = plannedOrder,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an outer join") {
      // Left outer hash join can only maintain ASC order
      assume(sortOrder == Ascending)
      val plan =
        new givenConfig {
          indexOn("A", "prop").providesOrder(orderCapability)
          cardinality = mapCardinality {
            // Force the planner to start at b
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b") =>
              100.0
            case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b") => 20.0
          }
        } getLogicalPlanFor s"MATCH (b) OPTIONAL MATCH (a:A)-[r]->(b) USING JOIN ON b WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop $cypherToken"

      plan._1 should equal(
        Projection(
          LeftOuterHashJoin(
            Set(v"b"),
            AllNodesScan(v"b", Set.empty),
            Expand(
              nodeIndexSeek(
                "a:A(prop > 'foo')",
                indexOrder = plannedOrder
              ),
              v"a",
              SemanticDirection.OUTGOING,
              Seq.empty,
              v"b",
              v"r"
            )
          ),
          Map(v"a.prop" -> prop("a", "prop"))
        )
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with a tail apply") {
      val plan =
        new givenConfig {
          indexOn("A", "prop").providesOrder(orderCapability)
        } getLogicalPlanFor
          s"""MATCH (a:A) WHERE a.prop > 'foo' WITH a SKIP 0
             |MATCH (b)
             |RETURN a.prop, b ORDER BY a.prop $cypherToken""".stripMargin

      plan._1 should equal(
        Projection(
          Apply(
            Skip(
              nodeIndexSeek(
                "a:A(prop > 'foo')",
                indexOrder = plannedOrder
              ),
              literalInt(0)
            ),
            AllNodesScan(v"b", Set(v"a"))
          ),
          Map(v"a.prop" -> prop("a", "prop"))
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (undirected) in a plan with a tail apply"
    ) {
      val query =
        s"""MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL WITH r SKIP 0
           |MATCH (c)
           |RETURN r.prop, c ORDER BY r.prop $cypherToken""".stripMargin

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .apply()
        .|.allNodeScan("c", "r")
        .cacheProperties("cacheRFromStore[r.prop]")
        .skip(0)
        .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", indexOrder = plannedOrder, indexType = IndexType.RANGE)
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (undirected) in a plan with a tail apply (seek)"
    ) {
      val query =
        s"""MATCH (a)-[r:REL]-(b) WHERE r.prop > 123 WITH r SKIP 0
           |MATCH (c)
           |RETURN r.prop, c ORDER BY r.prop $cypherToken""".stripMargin

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 100)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .apply()
        .|.allNodeScan("c", "r")
        .cacheProperties("cacheRFromStore[r.prop]")
        .skip(0)
        .relationshipIndexOperator(
          "(a)-[r:REL(prop > 123)]-(b)",
          indexOrder = plannedOrder,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (scan) in case of existence constraint"
    ) {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability)
          nodePropertyExistenceConstraintOn("Awesome", Set("prop"))
        } getLogicalPlanFor s"MATCH (n:Awesome) RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._1 should equal(
        Projection(
          nodeIndexSeek(
            "n:Awesome(prop)",
            indexOrder = plannedOrder
          ),
          Map(v"n.prop" -> prop("n", "prop"))
        )
      )
    }

    test(
      s"$cypherToken-$orderCapability: Order by index backed relationship property (undirected) should plan with provided order (scan) in case of existence constraint"
    ) {
      val query = s"MATCH (a)-[r:REL]-(b) RETURN r.prop ORDER BY r.prop $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(100)
        .setRelationshipCardinality("()-[:REL]-()", 10)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, providesOrder = orderCapability)
        .addRelationshipExistenceConstraint("REL", "prop")
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]-(b)",
          indexOrder = plannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(s"$cypherToken-$orderCapability: Should use OrderedDistinct if there is an ordered index available") {
      val plan = new givenConfig {
        indexOn("A", "prop")
          .providesOrder(orderCapability)
          .providesValues()
      }.getLogicalPlanFor("MATCH (a:A) WHERE a.prop IS NOT NULL RETURN DISTINCT a.prop")._1

      // We should prefer ASC index order if we can choose between both
      val expectedPlannedOrder = IndexOrderAscending

      val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
        .orderedDistinct(Seq("cache[a.prop]"), "cache[a.prop] AS `a.prop`")
        .nodeIndexOperator(
          "a:A(prop)",
          indexOrder = expectedPlannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build()

      plan shouldEqual expectedPlan
    }

    test(
      s"$cypherToken-$orderCapability: Should use OrderedDistinct if there is an ordered index available (multiple columns)"
    ) {
      val plan = new givenConfig {
        indexOn("A", "prop")
          .providesOrder(orderCapability)
          .providesValues()
      }.getLogicalPlanFor("MATCH (a:A) WHERE a.prop IS NOT NULL RETURN DISTINCT a.foo, a.prop")._1

      // We should prefer ASC index order if we can choose between both
      val expectedPlannedOrder = IndexOrderAscending

      val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
        .orderedDistinct(Seq("cache[a.prop]"), "a.foo AS `a.foo`", "cache[a.prop] AS `a.prop`")
        .nodeIndexOperator(
          "a:A(prop)",
          indexOrder = expectedPlannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build()

      plan shouldEqual expectedPlan
    }

    test(
      s"$cypherToken-$orderCapability: Should use OrderedDistinct if there is an ordered composite index available"
    ) {
      val plan = new givenConfig {
        indexOn("A", "foo", "prop")
          .providesOrder(orderCapability)
          .providesValues()
      }.getLogicalPlanFor("MATCH (a:A) WHERE a.prop IS NOT NULL AND a.foo IS NOT NULL RETURN DISTINCT a.foo, a.prop")._1

      // We should prefer ASC index order if we can choose between both
      val expectedPlannedOrder = IndexOrderAscending

      val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
        .orderedDistinct(Seq("cache[a.foo]", "cache[a.prop]"), "cache[a.foo] AS `a.foo`", "cache[a.prop] AS `a.prop`")
        .nodeIndexOperator(
          "a:A(foo, prop)",
          indexOrder = expectedPlannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build()

      plan shouldEqual expectedPlan
    }

    test(
      s"$cypherToken-$orderCapability: Should use OrderedDistinct if there is an ordered composite index available (reveresed column order)"
    ) {
      val plan = new givenConfig {
        indexOn("A", "foo", "prop")
          .providesOrder(orderCapability)
          .providesValues()
      }.getLogicalPlanFor("MATCH (a:A) WHERE a.prop IS NOT NULL AND a.foo IS NOT NULL RETURN DISTINCT a.prop, a.foo")._1

      // We should prefer ASC index order if we can choose between both
      val expectedPlannedOrder = IndexOrderAscending

      val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
        .orderedDistinct(Seq("cache[a.foo]", "cache[a.prop]"), "cache[a.foo] AS `a.foo`", "cache[a.prop] AS `a.prop`")
        .nodeIndexOperator(
          "a:A(foo, prop)",
          indexOrder = expectedPlannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build()

      plan shouldEqual expectedPlan
    }

    test(
      s"$cypherToken-$orderCapability: Should use OrderedAggregation if there is an ordered index available, in presence of ORDER BY for aggregating column"
    ) {
      val plan = new givenConfig {
        indexOn("A", "prop")
          .providesOrder(orderCapability)
          .providesValues()
      }.getLogicalPlanFor(s"MATCH (a:A) WHERE a.prop > 0 RETURN a.prop, count(*) AS c ORDER BY c $cypherToken")._1

      // We should prefer ASC index order if we can choose between both
      val expectedPlannedOrder = IndexOrderAscending

      val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
        .sortColumns(Seq(sortOrder("c")))
        .orderedAggregation(Seq("cache[a.prop] AS `a.prop`"), Seq("count(*) AS c"), Seq("cache[a.prop]"))
        .nodeIndexOperator(
          "a:A(prop > 0)",
          indexOrder = expectedPlannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build()

      plan shouldEqual expectedPlan
    }

    test(
      s"$cypherToken-$orderCapability: Should use OrderedAggregation if there is an ordered relationship index available, in presence of ORDER BY for aggregating column"
    ) {
      val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r.prop, count(*) AS c ORDER BY c $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(10)
        .setRelationshipCardinality("()-[:REL]-()", 10)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      // We should prefer ASC index order if we can choose between both
      val expectedPlannedOrder = IndexOrderAscending

      plan should equal(planner.subPlanBuilder()
        .sortColumns(Seq(sortOrder("c")))
        .orderedAggregation(Seq("cacheR[r.prop] AS `r.prop`"), Seq("count(*) AS c"), Seq("cacheR[r.prop]"))
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]-(b)",
          indexOrder = expectedPlannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build())
    }

    test(
      s"$cypherToken-$orderCapability: Should use OrderedAggregation if there is an ordered relationship index available, in presence of ORDER BY for aggregating column (seek)"
    ) {
      val query = s"MATCH (a)-[r:REL]-(b) WHERE r.prop > 123 RETURN r.prop, count(*) AS c ORDER BY c $cypherToken"

      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(10)
        .setRelationshipCardinality("()-[:REL]-()", 10)
        .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(query)
        .stripProduceResults

      // We should prefer ASC index order if we can choose between both
      val expectedPlannedOrder = IndexOrderAscending

      plan should equal(planner.subPlanBuilder()
        .sortColumns(Seq(sortOrder("c")))
        .orderedAggregation(Seq("cacheR[r.prop] AS `r.prop`"), Seq("count(*) AS c"), Seq("cacheR[r.prop]"))
        .relationshipIndexOperator(
          "(a)-[r:REL(prop > 123)]-(b)",
          indexOrder = expectedPlannedOrder,
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build())
    }

  }

  // Composite index

  private def compositeRelIndexPlannerBuilder() =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)

  // (orderByString, orderCapability, indexOrder, shouldFullSort, sortOnOnlyOne, sortItems, shouldPartialSort, alreadySorted)
  private def compositeIndexOnRangeTestData(variable: String)
    : Seq[(String, IndexOrderCapability, IndexOrder, Boolean, Boolean, Seq[ColumnOrder], Boolean, Seq[ColumnOrder])] =
    Seq(
      (
        s"$variable.prop1 DESC",
        NONE,
        IndexOrderNone,
        true,
        true,
        Seq(Descending(v"$variable.prop1")),
        false,
        Seq.empty
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 ASC",
        NONE,
        IndexOrderNone,
        true,
        false,
        Seq(Descending(v"$variable.prop1"), Ascending(v"$variable.prop2")),
        false,
        Seq.empty
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 DESC",
        NONE,
        IndexOrderNone,
        true,
        false,
        Seq(Descending(v"$variable.prop1"), Descending(v"$variable.prop2")),
        false,
        Seq.empty
      ),
      (
        s"$variable.prop1 ASC",
        NONE,
        IndexOrderNone,
        true,
        true,
        Seq(Ascending(v"$variable.prop1")),
        false,
        Seq.empty
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 ASC",
        NONE,
        IndexOrderNone,
        true,
        false,
        Seq(Ascending(v"$variable.prop1"), Ascending(v"$variable.prop2")),
        false,
        Seq.empty
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 DESC",
        NONE,
        IndexOrderNone,
        true,
        false,
        Seq(Ascending(v"$variable.prop1"), Descending(v"$variable.prop2")),
        false,
        Seq.empty
      ),
      (s"$variable.prop1 ASC", BOTH, IndexOrderAscending, false, false, Seq.empty, false, Seq.empty),
      (s"$variable.prop1 DESC", BOTH, IndexOrderDescending, false, false, Seq.empty, false, Seq.empty),
      (
        s"$variable.prop1 ASC, $variable.prop2 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        false,
        Seq.empty,
        false,
        Seq.empty
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 DESC",
        BOTH,
        IndexOrderAscending,
        false,
        false,
        Seq(Descending(v"$variable.prop2")),
        true,
        Seq(Ascending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        false,
        Seq(Ascending(v"$variable.prop2")),
        true,
        Seq(Descending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        false,
        Seq.empty,
        false,
        Seq.empty
      )
    )

  test("Order by index backed for composite node index on range") {
    val projectionBoth = Map[LogicalVariable, Expression](
      v"n.prop1" -> cachedNodeProp("n", "prop1"),
      v"n.prop2" -> cachedNodeProp("n", "prop2")
    )
    val projectionProp1 = Map[LogicalVariable, Expression](v"n.prop1" -> cachedNodeProp("n", "prop1"))
    val projectionProp2 = Map[LogicalVariable, Expression](v"n.prop2" -> cachedNodeProp("n", "prop2"))

    val expr = ands(lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3)))

    compositeIndexOnRangeTestData("n").foreach {
      case t @ (
          orderByString,
          orderCapability,
          indexOrder,
          shouldFullSort,
          sortOnOnlyOne,
          sortItems,
          shouldPartialSort,
          alreadySorted
        ) => withClue(t) {
          // When
          val query =
            s"""MATCH (n:Label)
               |WHERE n.prop1 >= 42 AND n.prop2 <= 3
               |RETURN n.prop1, n.prop2
               |ORDER BY $orderByString""".stripMargin
          val plan =
            new givenConfig {
              indexOn("Label", "prop1", "prop2").providesOrder(orderCapability).providesValues()
            } getLogicalPlanFor query

          // Then
          val leafPlan =
            nodeIndexSeek("n:Label(prop1 >= 42, prop2 <= 3)", indexOrder = indexOrder, getValue = _ => GetValue)
          plan._1 should equal {
            if (shouldPartialSort)
              PartialSort(Projection(Selection(expr, leafPlan), projectionBoth), alreadySorted, sortItems)
            else if (shouldFullSort && sortOnOnlyOne)
              Projection(
                Sort(Projection(Selection(expr, leafPlan), projectionProp1), sortItems),
                projectionProp2
              )
            else if (shouldFullSort && !sortOnOnlyOne)
              Sort(Projection(Selection(expr, leafPlan), projectionBoth), sortItems)
            else
              Projection(Selection(expr, leafPlan), projectionBoth)
          }
        }
    }
  }

  test("Order by index backed for composite relationship index on range") {
    val projectionBoth = Map(
      "r.prop1" -> cachedRelProp("r", "prop1"),
      "r.prop2" -> cachedRelProp("r", "prop2")
    )
    val projectionProp1 = Map("r.prop1" -> cachedRelProp("r", "prop1"))
    val projectionProp2 = Map("r.prop2" -> cachedRelProp("r", "prop2"))

    compositeIndexOnRangeTestData("r").foreach {
      case t @ (
          orderByString,
          orderCapability,
          indexOrder,
          shouldFullSort,
          sortOnOnlyOne,
          sortItems,
          shouldPartialSort,
          alreadySorted
        ) => withClue(t) {
          // When
          val query =
            s"""MATCH (a)-[r:REL]->(b)
               |WHERE r.prop1 >= 42 AND r.prop2 <= 3
               |RETURN r.prop1, r.prop2
               |ORDER BY $orderByString""".stripMargin

          val planner = compositeRelIndexPlannerBuilder()
            .addRelationshipIndex(
              "REL",
              Seq("prop1", "prop2"),
              1.0,
              0.01,
              withValues = true,
              providesOrder = orderCapability
            )
            .build()

          val plan = planner.plan(query).stripProduceResults

          // Then
          val expectedPlan = {
            val planBuilderWithSorting = {
              if (shouldPartialSort)
                planner.subPlanBuilder()
                  .partialSortColumns(alreadySorted, sortItems)
                  .projection(projectionBoth)
              else if (shouldFullSort && sortOnOnlyOne)
                planner.subPlanBuilder()
                  .projection(projectionProp2)
                  .sortColumns(sortItems)
                  .projection(projectionProp1)
              else if (shouldFullSort && !sortOnOnlyOne)
                planner.subPlanBuilder()
                  .sortColumns(sortItems)
                  .projection(projectionBoth)
              else
                planner.subPlanBuilder()
                  .projection(projectionBoth)
            }

            planBuilderWithSorting
              .filter("cacheR[r.prop2] <= 3")
              .relationshipIndexOperator(
                "(a)-[r:REL(prop1 >= 42, prop2 <= 3)]->(b)",
                indexOrder = indexOrder,
                getValue = _ => GetValue,
                indexType = IndexType.RANGE
              )
              .build()
          }

          withClue(query) {
            plan shouldBe expectedPlan
          }
        }
    }
  }

  // (orderByString, orderCapability, indexOrder, alreadySorted, toBeSorted)
  private def compositeIndexPartialOrderByTestData(variable: String) = {
    val asc = Seq(Ascending(v"$variable.prop1"), Ascending(v"$variable.prop2"))
    val ascProp3 = Seq(Ascending(v"$variable.prop3"))
    val desc = Seq(Descending(v"$variable.prop1"), Descending(v"$variable.prop2"))
    val descProp3 = Seq(Descending(v"$variable.prop3"))

    Seq(
      // Both index
      (s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC", BOTH, IndexOrderAscending, asc, ascProp3),
      (s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 DESC", BOTH, IndexOrderAscending, asc, descProp3),
      (s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 ASC", BOTH, IndexOrderDescending, desc, ascProp3),
      (s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC", BOTH, IndexOrderDescending, desc, descProp3)
    )
  }

  test("Order by partially index backed for composite node index on part of the order by") {
    compositeIndexPartialOrderByTestData("n").foreach {
      case (orderByString, orderCapability, indexOrder, alreadySorted, toBeSorted) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3
             |RETURN n.prop1, n.prop2, n.prop3
             |ORDER BY $orderByString""".stripMargin
        val plan =
          new givenConfig {
            indexOn("Label", "prop1", "prop2").providesOrder(orderCapability).providesValues()
          } getLogicalPlanFor query

        // Then
        plan._1 should equal(
          PartialSort(
            Projection(
              Selection(
                ands(lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3))),
                nodeIndexSeek("n:Label(prop1 >= 42, prop2)", indexOrder = indexOrder, getValue = _ => GetValue)
              ),
              Map(
                v"n.prop1" -> cachedNodeProp("n", "prop1"),
                v"n.prop2" -> cachedNodeProp("n", "prop2"),
                v"n.prop3" -> prop("n", "prop3")
              )
            ),
            alreadySorted,
            toBeSorted
          )
        )
    }
  }

  test("Order by partially index backed for composite relationship index on part of the order by") {
    compositeIndexPartialOrderByTestData("r").foreach {
      case (orderByString, orderCapability, indexOrder, alreadySorted, toBeSorted) =>
        // When
        val query =
          s"""MATCH (a)-[r:REL]->(b)
             |WHERE r.prop1 >= 42 AND r.prop2 <= 3
             |RETURN r.prop1, r.prop2, r.prop3
             |ORDER BY $orderByString""".stripMargin

        val planner = compositeRelIndexPlannerBuilder()
          .addRelationshipIndex(
            "REL",
            Seq("prop1", "prop2"),
            1.0,
            0.01,
            withValues = true,
            providesOrder = orderCapability
          )
          .build()

        val plan = planner.plan(query).stripProduceResults

        // Then
        withClue(query) {
          plan shouldBe planner.subPlanBuilder()
            .partialSortColumns(alreadySorted, toBeSorted)
            .projection("cacheR[r.prop1] AS `r.prop1`", "cacheR[r.prop2] AS `r.prop2`", "r.prop3 AS `r.prop3`")
            .filter("cacheR[r.prop2] <= 3")
            .relationshipIndexOperator(
              "(a)-[r:REL(prop1 >= 42, prop2)]->(b)",
              indexOrder = indexOrder,
              getValue = _ => GetValue,
              indexType = IndexType.RANGE
            )
            .build()
        }
    }
  }

  // (orderByString, orderCapability, indexOrder, fullSort, sortItems, partialSort, alreadySorted)
  private def compositeIndexOrderByMorePropsTestData(variable: String)
    : Seq[(String, IndexOrderCapability, IndexOrder, Boolean, Seq[ColumnOrder], Boolean, Seq[ColumnOrder])] = {
    val ascAll = Seq(
      Ascending(v"$variable.prop1"),
      Ascending(v"$variable.prop2"),
      Ascending(v"$variable.prop3"),
      Ascending(v"$variable.prop4")
    )
    val descAll = Seq(
      Descending(v"$variable.prop1"),
      Descending(v"$variable.prop2"),
      Descending(v"$variable.prop3"),
      Descending(v"$variable.prop4")
    )
    val asc1_2 = Seq(Ascending(v"$variable.prop1"), Ascending(v"$variable.prop2"))
    val desc1_2 = Seq(Descending(v"$variable.prop1"), Descending(v"$variable.prop2"))

    Seq(
      (
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC, $variable.prop4 DESC",
        NONE,
        IndexOrderNone,
        true,
        descAll,
        false,
        Seq.empty
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC, $variable.prop4 ASC",
        NONE,
        IndexOrderNone,
        true,
        ascAll,
        false,
        Seq.empty
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC, $variable.prop4 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        Seq.empty,
        false,
        Seq.empty
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC, $variable.prop4 DESC",
        BOTH,
        IndexOrderAscending,
        false,
        Seq(Descending(v"$variable.prop4")),
        true,
        Seq(
          Ascending(v"$variable.prop1"),
          Ascending(v"$variable.prop2"),
          Ascending(v"$variable.prop3")
        )
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 DESC, $variable.prop4 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        Seq(Descending(v"$variable.prop3"), Ascending(v"$variable.prop4")),
        true,
        asc1_2
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 DESC, $variable.prop4 DESC",
        BOTH,
        IndexOrderAscending,
        false,
        Seq(Descending(v"$variable.prop3"), Descending(v"$variable.prop4")),
        true,
        asc1_2
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 DESC, $variable.prop3 ASC, $variable.prop4 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        Seq(
          Descending(v"$variable.prop2"),
          Ascending(v"$variable.prop3"),
          Ascending(v"$variable.prop4")
        ),
        true,
        Seq(Ascending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 DESC, $variable.prop3 ASC, $variable.prop4 DESC",
        BOTH,
        IndexOrderAscending,
        false,
        Seq(
          Descending(v"$variable.prop2"),
          Ascending(v"$variable.prop3"),
          Descending(v"$variable.prop4")
        ),
        true,
        Seq(Ascending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 DESC, $variable.prop3 DESC, $variable.prop4 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        Seq(
          Descending(v"$variable.prop2"),
          Descending(v"$variable.prop3"),
          Ascending(v"$variable.prop4")
        ),
        true,
        Seq(Ascending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 DESC, $variable.prop3 DESC, $variable.prop4 DESC",
        BOTH,
        IndexOrderAscending,
        false,
        Seq(
          Descending(v"$variable.prop2"),
          Descending(v"$variable.prop3"),
          Descending(v"$variable.prop4")
        ),
        true,
        Seq(Ascending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC, $variable.prop4 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        Seq(Ascending(v"$variable.prop4")),
        true,
        Seq(
          Descending(v"$variable.prop1"),
          Descending(v"$variable.prop2"),
          Descending(v"$variable.prop3")
        )
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 ASC, $variable.prop4 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        Seq(Ascending(v"$variable.prop3"), Descending(v"$variable.prop4")),
        true,
        desc1_2
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 ASC, $variable.prop4 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        Seq(Ascending(v"$variable.prop3"), Ascending(v"$variable.prop4")),
        true,
        desc1_2
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 ASC, $variable.prop3 DESC, $variable.prop4 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        Seq(
          Ascending(v"$variable.prop2"),
          Descending(v"$variable.prop3"),
          Descending(v"$variable.prop4")
        ),
        true,
        Seq(Descending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 ASC, $variable.prop3 DESC, $variable.prop4 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        Seq(
          Ascending(v"$variable.prop2"),
          Descending(v"$variable.prop3"),
          Ascending(v"$variable.prop4")
        ),
        true,
        Seq(Descending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 ASC, $variable.prop3 ASC, $variable.prop4 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        Seq(
          Ascending(v"$variable.prop2"),
          Ascending(v"$variable.prop3"),
          Descending(v"$variable.prop4")
        ),
        true,
        Seq(Descending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 ASC, $variable.prop3 ASC, $variable.prop4 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        Seq(
          Ascending(v"$variable.prop2"),
          Ascending(v"$variable.prop3"),
          Ascending(v"$variable.prop4")
        ),
        true,
        Seq(Descending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC, $variable.prop4 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        Seq.empty,
        false,
        Seq.empty
      )
    )
  }

  test("Order by index backed for composite node index on more properties") {
    val expr = ands(
      lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3)),
      greaterThan(cachedNodeProp("n", "prop3"), literalString("a")),
      lessThan(cachedNodeProp("n", "prop4"), literalString("f"))
    )
    val projectionsAll = Map[LogicalVariable, Expression](
      v"n.prop1" -> cachedNodeProp("n", "prop1"),
      v"n.prop2" -> cachedNodeProp("n", "prop2"),
      v"n.prop3" -> cachedNodeProp("n", "prop3"),
      v"n.prop4" -> cachedNodeProp("n", "prop4")
    )

    compositeIndexOrderByMorePropsTestData("n").foreach {
      case (orderByString, orderCapability, indexOrder, fullSort, sortItems, partialSort, alreadySorted) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3 AND n.prop3 > 'a' AND n.prop4 < 'f'
             |RETURN n.prop1, n.prop2, n.prop3, n.prop4
             |ORDER BY $orderByString""".stripMargin
        val plan =
          new givenConfig {
            indexOn("Label", "prop1", "prop2", "prop3", "prop4").providesOrder(orderCapability).providesValues()
          } getLogicalPlanFor query

        // Then
        val leafPlan = nodeIndexSeek(
          "n:Label(prop1 >= 42, prop2 <= 3, prop3 > 'a', prop4 < 'f')",
          indexOrder = indexOrder,
          getValue = _ => GetValue
        )
        plan._1 should equal {
          if (fullSort)
            Sort(Projection(Selection(expr, leafPlan), projectionsAll), sortItems)
          else if (partialSort)
            PartialSort(Projection(Selection(expr, leafPlan), projectionsAll), alreadySorted, sortItems)
          else
            Projection(Selection(expr, leafPlan), projectionsAll)
        }
    }
  }

  test("Order by index backed for composite relationship index on more properties") {
    compositeIndexOrderByMorePropsTestData("r").foreach {
      case (orderByString, orderCapability, indexOrder, fullSort, sortItems, partialSort, alreadySorted) =>
        // When
        val query =
          s"""MATCH (a)-[r:REL]->(b)
             |WHERE r.prop1 >= 42 AND r.prop2 <= 3 AND r.prop3 > 'a' AND r.prop4 < 'f'
             |RETURN r.prop1, r.prop2, r.prop3, r.prop4
             |ORDER BY $orderByString""".stripMargin

        val planner = compositeRelIndexPlannerBuilder()
          .addRelationshipIndex(
            "REL",
            Seq("prop1", "prop2", "prop3", "prop4"),
            1.0,
            0.01,
            withValues = true,
            providesOrder = orderCapability
          )
          .build()

        val plan = planner.plan(query).stripProduceResults

        // Then

        val expectedPlan = {
          val planBuilderWithSorting =
            if (fullSort) planner.subPlanBuilder().sortColumns(sortItems)
            else if (partialSort) planner.subPlanBuilder().partialSortColumns(alreadySorted, sortItems)
            else planner.subPlanBuilder()

          planBuilderWithSorting
            .projection(
              "cacheR[r.prop1] AS `r.prop1`",
              "cacheR[r.prop2] AS `r.prop2`",
              "cacheR[r.prop3] AS `r.prop3`",
              "cacheR[r.prop4] AS `r.prop4`"
            )
            .filter("cacheR[r.prop2] <= 3", "cacheR[r.prop3] > 'a'", "cacheR[r.prop4] < 'f'")
            .relationshipIndexOperator(
              "(a)-[r:REL(prop1 >= 42, prop2 <= 3, prop3 > 'a', prop4 < 'f')]->(b)",
              indexOrder = indexOrder,
              getValue = _ => GetValue,
              indexType = IndexType.RANGE
            )
            .build()
        }

        withClue(query) {
          plan shouldBe expectedPlan
        }
    }
  }

  // (orderByString, orderCapability, indexOrder, sort, projections, projectionsAfterSort, toSort, alreadySorted)
  private def compositeIndexOrderByPrefixTestData(
    variable: String,
    cachedEntityProperty: (String, String) => CachedProperty
  ) = {
    val projectionsAll = Map(
      s"$variable.prop1" -> cachedEntityProperty(variable, "prop1"),
      s"$variable.prop2" -> cachedEntityProperty(variable, "prop2"),
      s"$variable.prop3" -> cachedEntityProperty(variable, "prop3"),
      s"$variable.prop4" -> cachedEntityProperty(variable, "prop4")
    )
    val projections_1_2_4 = Map(
      s"$variable.prop1" -> cachedEntityProperty(variable, "prop1"),
      s"$variable.prop2" -> cachedEntityProperty(variable, "prop2"),
      s"$variable.prop4" -> cachedEntityProperty(variable, "prop4")
    )
    val projections_1_3_4 = Map(
      s"$variable.prop1" -> cachedEntityProperty(variable, "prop1"),
      s"$variable.prop3" -> cachedEntityProperty(variable, "prop3"),
      s"$variable.prop4" -> cachedEntityProperty(variable, "prop4")
    )

    Seq(
      (
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        projectionsAll,
        Map.empty[String, Expression],
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop4 ASC",
        BOTH,
        IndexOrderAscending,
        true,
        projections_1_2_4,
        Map(s"$variable.prop3" -> cachedEntityProperty(variable, "prop3")),
        Seq(Ascending(v"$variable.prop4")),
        Seq(Ascending(v"$variable.prop1"), Ascending(v"$variable.prop2"))
      ),
      (
        s"$variable.prop1 ASC, $variable.prop3 ASC, $variable.prop4 ASC",
        BOTH,
        IndexOrderAscending,
        true,
        projections_1_3_4,
        Map(s"$variable.prop2" -> cachedEntityProperty(variable, "prop2")),
        Seq(Ascending(v"$variable.prop3"), Ascending(v"$variable.prop4")),
        Seq(Ascending(v"$variable.prop1"))
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        projectionsAll,
        Map.empty[String, Expression],
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop4 DESC",
        BOTH,
        IndexOrderDescending,
        true,
        projections_1_2_4,
        Map(s"$variable.prop3" -> cachedEntityProperty(variable, "prop3")),
        Seq(Descending(v"$variable.prop4")),
        Seq(Descending(v"$variable.prop1"), Descending(v"$variable.prop2"))
      ),
      (
        s"$variable.prop1 DESC, $variable.prop3 DESC, $variable.prop4 DESC",
        BOTH,
        IndexOrderDescending,
        true,
        projections_1_3_4,
        Map(s"$variable.prop2" -> cachedEntityProperty(variable, "prop2")),
        Seq(Descending(v"$variable.prop3"), Descending(v"$variable.prop4")),
        Seq(Descending(v"$variable.prop1"))
      )
    )
  }

  test("Order by index backed for composite node index on more properties than is ordered on") {
    val expr = ands(
      lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3)),
      greaterThan(cachedNodeProp("n", "prop3"), literalString("a")),
      lessThan(cachedNodeProp("n", "prop4"), literalString("f"))
    )
    compositeIndexOrderByPrefixTestData("n", cachedNodeProp).foreach {
      case (
          orderByString,
          orderCapability,
          indexOrder,
          sort,
          projections,
          projectionsAfterSort,
          toSort,
          alreadySorted
        ) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3 AND n.prop3 > 'a' AND n.prop4 < 'f'
             |RETURN n.prop1, n.prop2, n.prop3, n.prop4
             |ORDER BY $orderByString""".stripMargin
        val plan =
          new givenConfig {
            indexOn("Label", "prop1", "prop2", "prop3", "prop4").providesOrder(orderCapability).providesValues()
          } getLogicalPlanFor query

        // Then
        val leafPlan = nodeIndexSeek(
          "n:Label(prop1 >= 42, prop2 <= 3, prop3 > 'a', prop4 < 'f')",
          indexOrder = indexOrder,
          getValue = _ => GetValue
        )
        plan._1 should equal {
          if (sort)
            Projection(
              PartialSort(
                Projection(
                  Selection(expr, leafPlan),
                  projections.map { case (key, value) => varFor(key) -> value }
                ),
                alreadySorted,
                toSort
              ),
              projectionsAfterSort.map { case (key, value) => varFor(key) -> value }
            )
          else
            Projection(
              Selection(expr, leafPlan),
              projections.map { case (key, value) => varFor(key) -> value }
            )
        }
    }
  }

  test("Order by index backed for composite relationship index on more properties than is ordered on") {
    compositeIndexOrderByPrefixTestData("r", cachedRelProp).foreach {
      case (
          orderByString,
          orderCapability,
          indexOrder,
          sort,
          projections,
          projectionsAfterSort,
          toSort,
          alreadySorted
        ) =>
        // When
        val query =
          s"""MATCH (a)-[r:REL]->(b)
             |WHERE r.prop1 >= 42 AND r.prop2 <= 3 AND r.prop3 > 'a' AND r.prop4 < 'f'
             |RETURN r.prop1, r.prop2, r.prop3, r.prop4
             |ORDER BY $orderByString""".stripMargin

        val planner = compositeRelIndexPlannerBuilder()
          .addRelationshipIndex(
            "REL",
            Seq("prop1", "prop2", "prop3", "prop4"),
            1.0,
            0.01,
            withValues = true,
            providesOrder = orderCapability
          )
          .build()

        val plan = planner.plan(query).stripProduceResults

        // Then
        val expectedPlan = {
          val planBuilderWithSort =
            if (sort)
              planner.subPlanBuilder()
                .projection(projectionsAfterSort)
                .partialSortColumns(alreadySorted, toSort)
                .projection(projections)
            else
              planner.subPlanBuilder()
                .projection(projections)

          planBuilderWithSort
            .filter("cacheR[r.prop2] <= 3", "cacheR[r.prop3] > 'a'", "cacheR[r.prop4] < 'f'")
            .relationshipIndexOperator(
              "(a)-[r:REL(prop1 >= 42, prop2 <= 3, prop3 > 'a', prop4 < 'f')]->(b)",
              indexOrder = indexOrder,
              getValue = _ => GetValue,
              indexType = IndexType.RANGE
            )
            .build()
        }

        withClue(query) {
          plan shouldBe expectedPlan
        }
    }
  }

  // (returnString, orderByString, orderCapability, indexOrder, fullSort, partialSort, returnProjections, sortProjections, selectionExpression, sortItems, alreadySorted)
  private def compositeIndexReturnOrderByTestData(
    variable: String,
    cachedEntityProp: (String, String) => CachedProperty,
    cachedEntityPropFromStore: (String, String) => CachedProperty
  ): Seq[(
    String,
    String,
    IndexOrderCapability,
    IndexOrder,
    Boolean,
    Boolean,
    Map[String, LogicalProperty],
    Map[String, Expression],
    Ands,
    Seq[ColumnOrder],
    Seq[ColumnOrder]
  )] = {
    val expr = ands(
      lessThanOrEqual(prop(variable, "prop2"), literalInt(3)),
      greaterThan(prop(variable, "prop3"), literalString(""))
    )
    val expr_2_3_cached = ands(
      lessThanOrEqual(cachedEntityPropFromStore(variable, "prop2"), literalInt(3)),
      greaterThan(cachedEntityPropFromStore(variable, "prop3"), literalString(""))
    )
    val expr_2_cached = ands(
      lessThanOrEqual(cachedEntityPropFromStore(variable, "prop2"), literalInt(3)),
      greaterThan(prop(variable, "prop3"), literalString(""))
    )
    val expr_3_cached = ands(
      lessThanOrEqual(prop(variable, "prop2"), literalInt(3)),
      greaterThan(cachedEntityPropFromStore(variable, "prop3"), literalString(""))
    )
    val map_empty = Map.empty[String, Expression] // needed for correct type
    val map_1 = Map(s"$variable.prop1" -> prop(variable, "prop1"))
    val map_2_cached = Map(s"$variable.prop2" -> cachedEntityProp(variable, "prop2"))
    val map_3_cached = Map(s"$variable.prop3" -> cachedEntityProp(variable, "prop3"))
    val map_2_3_cached = Map(
      s"$variable.prop2" -> cachedEntityProp(variable, "prop2"),
      s"$variable.prop3" -> cachedEntityProp(variable, "prop3")
    )

    val asc_1 = Seq(Ascending(v"$variable.prop1"))
    val desc_1 = Seq(Descending(v"$variable.prop1"))
    val asc_3 = Seq(Ascending(v"$variable.prop3"))
    val desc_3 = Seq(Descending(v"$variable.prop3"))
    val asc_1_2 = Seq(Ascending(v"$variable.prop1"), Ascending(v"$variable.prop2"))
    val desc_1_2 = Seq(Descending(v"$variable.prop1"), Descending(v"$variable.prop2"))
    val asc_2_3 = Seq(Ascending(v"$variable.prop2"), Ascending(v"$variable.prop3"))
    val desc_2_asc_3 = Seq(Descending(v"$variable.prop2"), Ascending(v"$variable.prop3"))
    val asc_1_2_3 = Seq(
      Ascending(v"$variable.prop1"),
      Ascending(v"$variable.prop2"),
      Ascending(v"$variable.prop3")
    )
    val desc_1_2_3 = Seq(
      Descending(v"$variable.prop1"),
      Descending(v"$variable.prop2"),
      Descending(v"$variable.prop3")
    )

    Seq(
      (
        s"$variable.prop1",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        false,
        map_1,
        map_empty,
        expr,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop1",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_1,
        map_2_3_cached,
        expr_2_3_cached,
        desc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop2",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        false,
        map_2_cached,
        map_empty,
        expr_2_cached,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop2",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_2_cached,
        map_1 ++ map_3_cached,
        expr_2_3_cached,
        desc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop3",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        false,
        map_3_cached,
        map_empty,
        expr_3_cached,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop3",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_3_cached,
        map_1 ++ map_2_cached,
        expr_2_3_cached,
        desc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop1, $variable.prop2",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        false,
        map_1 ++ map_2_cached,
        map_empty,
        expr_2_cached,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop1, $variable.prop2",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_1 ++ map_2_cached,
        map_3_cached,
        expr_2_3_cached,
        desc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop1, $variable.prop3",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        false,
        map_1 ++ map_3_cached,
        map_empty,
        expr_3_cached,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop1, $variable.prop3",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_1 ++ map_3_cached,
        map_2_cached,
        expr_2_3_cached,
        desc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop1",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_1,
        map_2_3_cached,
        expr_2_3_cached,
        asc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop1",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        false,
        map_1,
        map_empty,
        expr,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop2",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_2_cached,
        map_1 ++ map_3_cached,
        expr_2_3_cached,
        asc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop2",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        false,
        map_2_cached,
        map_empty,
        expr_2_cached,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop3",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_3_cached,
        map_1 ++ map_2_cached,
        expr_2_3_cached,
        asc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop3",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        false,
        map_3_cached,
        map_empty,
        expr_3_cached,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop1, $variable.prop2",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_1 ++ map_2_cached,
        map_3_cached,
        expr_2_3_cached,
        asc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop1, $variable.prop2",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        false,
        map_1 ++ map_2_cached,
        map_empty,
        expr_2_cached,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop1, $variable.prop3",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 ASC",
        NONE,
        IndexOrderNone,
        true,
        false,
        map_1 ++ map_3_cached,
        map_2_cached,
        expr_2_3_cached,
        asc_1_2_3,
        Seq.empty
      ),
      (
        s"$variable.prop1, $variable.prop3",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 DESC",
        BOTH,
        IndexOrderDescending,
        false,
        false,
        map_1 ++ map_3_cached,
        map_empty,
        expr_3_cached,
        Seq.empty,
        Seq.empty
      ),
      (
        s"$variable.prop1",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 DESC",
        BOTH,
        IndexOrderAscending,
        false,
        true,
        map_1,
        map_2_3_cached,
        expr_2_3_cached,
        desc_3,
        asc_1_2
      ),
      (
        s"$variable.prop1",
        s"$variable.prop1 ASC, $variable.prop2 DESC, $variable.prop3 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        true,
        map_1,
        map_2_3_cached,
        expr_2_3_cached,
        desc_2_asc_3,
        asc_1
      ),
      (
        s"$variable.prop1",
        s"$variable.prop1 DESC, $variable.prop2 ASC, $variable.prop3 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        true,
        map_1,
        map_2_3_cached,
        expr_2_3_cached,
        asc_2_3,
        desc_1
      ),
      (
        s"$variable.prop1",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        true,
        map_1,
        map_2_3_cached,
        expr_2_3_cached,
        asc_3,
        desc_1_2
      ),
      (
        s"$variable.prop1, $variable.prop2",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 DESC",
        BOTH,
        IndexOrderAscending,
        false,
        true,
        map_1 ++ map_2_cached,
        map_3_cached,
        expr_2_3_cached,
        desc_3,
        asc_1_2
      ),
      (
        s"$variable.prop1, $variable.prop2",
        s"$variable.prop1 ASC, $variable.prop2 DESC, $variable.prop3 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        true,
        map_1 ++ map_2_cached,
        map_3_cached,
        expr_2_3_cached,
        desc_2_asc_3,
        asc_1
      ),
      (
        s"$variable.prop1, $variable.prop2",
        s"$variable.prop1 DESC, $variable.prop2 ASC, $variable.prop3 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        true,
        map_1 ++ map_2_cached,
        map_3_cached,
        expr_2_3_cached,
        asc_2_3,
        desc_1
      ),
      (
        s"$variable.prop1, $variable.prop2",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        true,
        map_1 ++ map_2_cached,
        map_3_cached,
        expr_2_3_cached,
        asc_3,
        desc_1_2
      ),
      (
        s"$variable.prop1, $variable.prop3",
        s"$variable.prop1 ASC, $variable.prop2 ASC, $variable.prop3 DESC",
        BOTH,
        IndexOrderAscending,
        false,
        true,
        map_1 ++ map_3_cached,
        map_2_cached,
        expr_2_3_cached,
        desc_3,
        asc_1_2
      ),
      (
        s"$variable.prop1, $variable.prop3",
        s"$variable.prop1 ASC, $variable.prop2 DESC, $variable.prop3 ASC",
        BOTH,
        IndexOrderAscending,
        false,
        true,
        map_1 ++ map_3_cached,
        map_2_cached,
        expr_2_3_cached,
        desc_2_asc_3,
        asc_1
      ),
      (
        s"$variable.prop1, $variable.prop3",
        s"$variable.prop1 DESC, $variable.prop2 ASC, $variable.prop3 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        true,
        map_1 ++ map_3_cached,
        map_2_cached,
        expr_2_3_cached,
        asc_2_3,
        desc_1
      ),
      (
        s"$variable.prop1, $variable.prop3",
        s"$variable.prop1 DESC, $variable.prop2 DESC, $variable.prop3 ASC",
        BOTH,
        IndexOrderDescending,
        false,
        true,
        map_1 ++ map_3_cached,
        map_2_cached,
        expr_2_3_cached,
        asc_3,
        desc_1_2
      )
    )
  }

  test("Order by index backed for composite node index when not returning same as order on") {
    compositeIndexReturnOrderByTestData("n", cachedNodeProp, cachedNodePropFromStore).foreach {
      case (
          returnString,
          orderByString,
          orderCapability,
          indexOrder,
          fullSort,
          partialSort,
          returnProjections,
          sortProjections,
          selectionExpression,
          sortItems,
          alreadySorted
        ) =>
        // When
        val query =
          s"""
             |MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3 AND n.prop3 > ''
             |RETURN $returnString
             |ORDER BY $orderByString
          """.stripMargin
        val plan =
          new givenConfig {
            indexOn("Label", "prop1", "prop2", "prop3").providesOrder(orderCapability)
          } getLogicalPlanFor query

        // Then
        val leafPlan = nodeIndexSeek("n:Label(prop1 >= 42, prop2 <= 3, prop3 > '')", indexOrder = indexOrder)
        withClue(query) {
          plan._1 should equal {
            if (fullSort)
              Sort(
                Projection(
                  Projection(
                    Selection(selectionExpression, leafPlan),
                    returnProjections.map { case (key, value) => varFor(key) -> value }
                  ),
                  sortProjections.map { case (key, value) => varFor(key) -> value }
                ),
                sortItems
              )
            else if (partialSort)
              PartialSort(
                Projection(
                  Projection(
                    Selection(selectionExpression, leafPlan),
                    returnProjections.map { case (key, value) => varFor(key) -> value }
                  ),
                  sortProjections.map { case (key, value) => varFor(key) -> value }
                ),
                alreadySorted,
                sortItems
              )
            else
              Projection(
                Selection(selectionExpression, leafPlan),
                returnProjections.map { case (key, value) => varFor(key) -> value }
              )
          }
        }
    }
  }

  test("Order by index backed for composite relationship index when not returning same as order on") {
    compositeIndexReturnOrderByTestData("r", cachedRelProp, cachedRelPropFromStore).foreach {
      case t @ (
          returnString,
          orderByString,
          orderCapability,
          indexOrder,
          fullSort,
          partialSort,
          returnProjections,
          sortProjections,
          selectionExpression,
          sortItems,
          alreadySorted
        ) => withClue(t) {
          // When
          val query =
            s"""
               |MATCH (a)-[r:REL]->(b)
               |WHERE r.prop1 >= 42 AND r.prop2 <= 3 AND r.prop3 > ''
               |RETURN $returnString
               |ORDER BY $orderByString
          """.stripMargin

          val planner = compositeRelIndexPlannerBuilder()
            .addRelationshipIndex("REL", Seq("prop1", "prop2", "prop3"), 1.0, 0.01, providesOrder = orderCapability)
            .build()

          val plan = planner.plan(query).stripProduceResults

          // Then
          val expectedPlan = {
            val planBuilderWithSort =
              if (fullSort)
                planner.subPlanBuilder()
                  .sortColumns(sortItems)
                  .projection(sortProjections)
                  .projection(returnProjections)
              else if (partialSort)
                planner.subPlanBuilder()
                  .partialSortColumns(alreadySorted, sortItems)
                  .projection(sortProjections)
                  .projection(returnProjections)
              else
                planner.subPlanBuilder()
                  .projection(returnProjections)

            planBuilderWithSort
              .filterExpression(selectionExpression.exprs.toSeq: _*)
              .relationshipIndexOperator(
                "(a)-[r:REL(prop1 >= 42, prop2 <= 3, prop3 > '')]->(b)",
                indexOrder = indexOrder,
                indexType = IndexType.RANGE
              )
              .build()
          }

          withClue(query) {
            plan shouldBe expectedPlan
          }
        }
    }
  }

  // (orderByString, orderCapability, indexOrder, shouldSort, alreadySorted, toBeSorted)
  private def compositeIndexOrderByDifferentDirectionsFirstPropTestData(variable: String)
    : Seq[(String, IndexOrderCapability, IndexOrder, Boolean, Seq[ColumnOrder], Seq[ColumnOrder])] = Seq(
    (s"$variable.prop1 ASC", BOTH, IndexOrderAscending, false, Seq.empty, Seq.empty),
    (
      s"$variable.prop1 DESC",
      BOTH,
      IndexOrderAscending,
      false,
      Seq.empty,
      Seq.empty
    ), // Index gives ASC, reports DESC, since ASC is cheaper
    (s"$variable.prop1 ASC, $variable.prop2 ASC", BOTH, IndexOrderAscending, false, Seq.empty, Seq.empty),
    (
      s"$variable.prop1 ASC, $variable.prop2 DESC",
      BOTH,
      IndexOrderDescending,
      false,
      Seq.empty,
      Seq.empty
    ), // Index gives DESC DESC, reports ASC DESC
    (
      s"$variable.prop1 DESC, $variable.prop2 ASC",
      BOTH,
      IndexOrderAscending,
      false,
      Seq.empty,
      Seq.empty
    ), // Index gives ASC ASC, reports DESC ASC
    (s"$variable.prop1 DESC, $variable.prop2 DESC", BOTH, IndexOrderDescending, false, Seq.empty, Seq.empty)
  )

  test(
    "Order by index backed for composite node index with different directions and equality predicate on first property"
  ) {
    val projection = Map[LogicalVariable, Expression](
      v"n.prop1" -> cachedNodeProp("n", "prop1"),
      v"n.prop2" -> cachedNodeProp("n", "prop2")
    )

    compositeIndexOrderByDifferentDirectionsFirstPropTestData("n").foreach {
      case (orderByString, orderCapability, indexOrder, shouldSort, alreadySorted, toBeSorted) =>
        withClue(s"ORDER BY $orderByString with index order capability $orderCapability:") {
          // When
          val query =
            s"""MATCH (n:Label)
               |WHERE n.prop1 = 42 AND n.prop2 <= 3
               |RETURN n.prop1, n.prop2
               |ORDER BY $orderByString""".stripMargin
          val plan =
            new givenConfig {
              indexOn("Label", "prop1", "prop2").providesOrder(orderCapability).providesValues()
            } getLogicalPlanFor query

          // Then
          val leafPlan =
            nodeIndexSeek("n:Label(prop1 = 42, prop2 <= 3)", indexOrder = indexOrder, getValue = _ => GetValue)
          plan._1 should equal {
            if (shouldSort)
              PartialSort(Projection(leafPlan, projection), alreadySorted, toBeSorted)
            else
              Projection(leafPlan, projection)
          }
        }
    }
  }

  test(
    "Order by index backed for composite relationship index with different directions and equality predicate on first property"
  ) {
    compositeIndexOrderByDifferentDirectionsFirstPropTestData("r").foreach {
      case (orderByString, orderCapability, indexOrder, shouldSort, alreadySorted, toBeSorted) =>
        withClue(s"ORDER BY $orderByString with index order capability $orderCapability:") {
          // When
          val query =
            s"""MATCH (a)-[r:REL]->(b)
               |WHERE r.prop1 = 42 AND r.prop2 <= 3
               |RETURN r.prop1, r.prop2
               |ORDER BY $orderByString""".stripMargin

          val planner = compositeRelIndexPlannerBuilder()
            .addRelationshipIndex(
              "REL",
              Seq("prop1", "prop2"),
              1.0,
              0.01,
              withValues = true,
              providesOrder = orderCapability
            )
            .build()

          val plan = planner.plan(query).stripProduceResults

          // Then
          val expectedPlan = {
            val planBuilderWithSort =
              if (shouldSort) planner.subPlanBuilder().partialSortColumns(alreadySorted, toBeSorted)
              else planner.subPlanBuilder()

            planBuilderWithSort
              .projection("cacheR[r.prop1] AS `r.prop1`", "cacheR[r.prop2] AS `r.prop2`")
              .relationshipIndexOperator(
                "(a)-[r:REL(prop1 = 42, prop2 <= 3)]->(b)",
                indexOrder = indexOrder,
                getValue = _ => GetValue,
                indexType = IndexType.RANGE
              )
              .build()
          }

          withClue(query) {
            plan shouldBe expectedPlan
          }
        }
    }
  }

  // (orderByString, orderCapability, indexOrder, shouldSort, toBeSorted)
  private def compositeIndexOrderByDifferentDirectionsSecondPropTestData(variable: String)
    : Seq[(String, IndexOrderCapability, IndexOrder, Boolean, Seq[ColumnOrder])] = Seq(
    (
      s"$variable.prop1 DESC, $variable.prop2 ASC",
      NONE,
      IndexOrderNone,
      true,
      Seq(Descending(v"$variable.prop1"), Ascending(v"$variable.prop2"))
    ),
    (
      s"$variable.prop1 DESC, $variable.prop2 DESC",
      NONE,
      IndexOrderNone,
      true,
      Seq(Descending(v"$variable.prop1"), Descending(v"$variable.prop2"))
    ),
    (
      s"$variable.prop1 ASC, $variable.prop2 ASC",
      NONE,
      IndexOrderNone,
      true,
      Seq(Ascending(v"$variable.prop1"), Ascending(v"$variable.prop2"))
    ),
    (
      s"$variable.prop1 ASC, $variable.prop2 DESC",
      NONE,
      IndexOrderNone,
      true,
      Seq(Ascending(v"$variable.prop1"), Descending(v"$variable.prop2"))
    ),
    (s"$variable.prop1 ASC, $variable.prop2 ASC", BOTH, IndexOrderAscending, false, Seq.empty),
    (
      s"$variable.prop1 ASC, $variable.prop2 DESC",
      BOTH,
      IndexOrderAscending,
      false,
      Seq.empty
    ), // Index gives ASC ASC, reports ASC DESC (true after filter at least)
    (
      s"$variable.prop1 DESC, $variable.prop2 ASC",
      BOTH,
      IndexOrderDescending,
      false,
      Seq.empty
    ), // Index gives DESC DESC, reports DESC ASC (true after filter at least)
    (s"$variable.prop1 DESC, $variable.prop2 DESC", BOTH, IndexOrderDescending, false, Seq.empty)
  )

  test(
    "Order by index backed for composite node index with different directions and equality predicate on second property"
  ) {
    val projection = Map[LogicalVariable, Expression](
      v"n.prop1" -> cachedNodeProp("n", "prop1"),
      v"n.prop2" -> cachedNodeProp("n", "prop2")
    )

    compositeIndexOrderByDifferentDirectionsSecondPropTestData("n").foreach {
      case (orderByString, orderCapability, indexOrder, shouldSort, toBeSorted) =>
        withClue(s"ORDER BY $orderByString with index order capability $orderCapability:") {
          // When
          val query =
            s"""MATCH (n:Label)
               |WHERE n.prop1 <= 42 AND n.prop2 = 3
               |RETURN n.prop1, n.prop2
               |ORDER BY $orderByString""".stripMargin
          val plan =
            new givenConfig {
              indexOn("Label", "prop1", "prop2").providesOrder(orderCapability).providesValues()
            } getLogicalPlanFor query

          // Then
          val leafPlan = Selection(
            ands(equals(cachedNodeProp("n", "prop2"), literalInt(3))),
            nodeIndexSeek("n:Label(prop1 <= 42, prop2 = 3)", indexOrder = indexOrder, getValue = _ => GetValue)
          )
          plan._1 should equal {
            if (shouldSort)
              Sort(Projection(leafPlan, projection), toBeSorted)
            else
              Projection(leafPlan, projection)
          }
        }
    }
  }

  test(
    "Order by index backed for composite relationship index with different directions and equality predicate on second property"
  ) {
    compositeIndexOrderByDifferentDirectionsSecondPropTestData("r").foreach {
      case (orderByString, orderCapability, indexOrder, shouldSort, toBeSorted) =>
        withClue(s"ORDER BY $orderByString with index order capability $orderCapability:") {
          // When
          val query =
            s"""MATCH (a)-[r:REL]->(b)
               |WHERE r.prop1 <= 42 AND r.prop2 = 3
               |RETURN r.prop1, r.prop2
               |ORDER BY $orderByString""".stripMargin

          val planner = compositeRelIndexPlannerBuilder()
            .addRelationshipIndex(
              "REL",
              Seq("prop1", "prop2"),
              1.0,
              0.01,
              withValues = true,
              providesOrder = orderCapability
            )
            .build()

          val plan = planner.plan(query).stripProduceResults

          // Then
          val projections = Seq("cacheR[r.prop1] AS `r.prop1`", "cacheR[r.prop2] AS `r.prop2`")
          val expectedPlan = {
            val planBuilderWithSort =
              if (shouldSort)
                planner.subPlanBuilder()
                  .sortColumns(toBeSorted)
                  .projection(projections: _*)
              else
                planner.subPlanBuilder()
                  .projection(projections: _*)

            planBuilderWithSort
              .filter("cacheR[r.prop2] = 3")
              .relationshipIndexOperator(
                "(a)-[r:REL(prop1 <= 42, prop2 = 3)]->(b)",
                indexOrder = indexOrder,
                getValue = _ => GetValue,
                indexType = IndexType.RANGE
              )
              .build()
          }

          withClue(query) {
            plan shouldBe expectedPlan
          }
        }
    }
  }

  // (orderByString, orderCapability, indexOrder)
  private def compositeIndexOrderByDifferentDirectionsBothPropsTestData(variable: String)
    : Seq[(String, IndexOrderCapability, IndexOrder)] = Seq(
    (s"$variable.prop1 ASC, $variable.prop2 ASC", BOTH, IndexOrderAscending),
    (s"$variable.prop1 ASC, $variable.prop2 DESC", BOTH, IndexOrderAscending), // Index gives ASC ASC, reports ASC DESC
    (s"$variable.prop1 DESC, $variable.prop2 ASC", BOTH, IndexOrderAscending), // Index gives ASC ASC, reports DESC ASC
    (s"$variable.prop1 DESC, $variable.prop2 DESC", BOTH, IndexOrderAscending) // Index gives ASC ASC, reports DESC DESC
  )

  test(
    "Order by index backed for composite node index with different directions and equality predicate on both properties"
  ) {
    val projection = Map[LogicalVariable, Expression](
      v"n.prop1" -> cachedNodeProp("n", "prop1"),
      v"n.prop2" -> cachedNodeProp("n", "prop2")
    )

    compositeIndexOrderByDifferentDirectionsBothPropsTestData("n").foreach {
      case (orderByString, orderCapability, indexOrder) =>
        withClue(s"ORDER BY $orderByString with index order capability $orderCapability:") {
          // When
          val query =
            s"""MATCH (n:Label)
               |WHERE n.prop1 = 42 AND n.prop2 = 3
               |RETURN n.prop1, n.prop2
               |ORDER BY $orderByString""".stripMargin
          val plan =
            new givenConfig {
              indexOn("Label", "prop1", "prop2").providesOrder(orderCapability).providesValues()
            } getLogicalPlanFor query

          plan._1 should equal(Projection(
            nodeIndexSeek("n:Label(prop1 = 42, prop2 = 3)", indexOrder = indexOrder, getValue = _ => GetValue),
            projection
          ))
        }
    }
  }

  test(
    "Order by index backed for composite relationship index with different directions and equality predicate on both properties"
  ) {
    compositeIndexOrderByDifferentDirectionsBothPropsTestData("r").foreach {
      case (orderByString, orderCapability, indexOrder) =>
        withClue(s"ORDER BY $orderByString with index order capability $orderCapability:") {
          // When
          val query =
            s"""MATCH (a)-[r:REL]->(b)
               |WHERE r.prop1 = 42 AND r.prop2 = 3
               |RETURN r.prop1, r.prop2
               |ORDER BY $orderByString""".stripMargin
          val planner = compositeRelIndexPlannerBuilder()
            .addRelationshipIndex(
              "REL",
              Seq("prop1", "prop2"),
              1.0,
              0.01,
              withValues = true,
              providesOrder = orderCapability
            )
            .build()

          val plan = planner.plan(query).stripProduceResults

          // Then
          withClue(query) {
            plan shouldBe planner.subPlanBuilder()
              .projection("cacheR[r.prop1] AS `r.prop1`", "cacheR[r.prop2] AS `r.prop2`")
              .relationshipIndexOperator(
                "(a)-[r:REL(prop1 = 42, prop2 = 3)]->(b)",
                indexOrder = indexOrder,
                getValue = _ => GetValue,
                indexType = IndexType.RANGE
              )
              .build()
          }
        }
    }
  }

  // Min and Max

  private def relationshipIndexMinMaxSetup = plannerBuilder()
    .setAllNodesCardinality(500)
    .setAllRelationshipsCardinality(500)
    .setRelationshipCardinality("()-[:REL]->()", 100)

  for (
    (TestOrder(plannedOrder, cypherToken, orderCapability, _), functionName) <-
      List((ASCENDING_BOTH, "min"), (DESCENDING_BOTH, "max"), (ASCENDING_BOTH, "miN"), (DESCENDING_BOTH, "maX"))
  ) {

    // Node label scan

    test(s"$orderCapability-$functionName: should use node label scan order") {
      val plan =
        new givenConfig().getLogicalPlanFor(s"MATCH (n:Awesome) RETURN $functionName(n)", stripProduceResults = false)

      plan._1 should equal(
        new LogicalPlanBuilder()
          .produceResults(s"`$functionName(n)`")
          .optional()
          .limit(1)
          .projection(s"n AS `$functionName(n)`")
          .nodeByLabelScan("n", "Awesome", plannedOrder)
          .build()
      )
    }

    // Node property index scan

    test(s"$orderCapability-$functionName: should use provided node index scan order") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop IS NOT NULL RETURN $functionName(n.prop)"

      plan._1 should equal(
        Optional(
          Limit(
            Projection(
              NodeIndexScan(
                v"n",
                LabelToken("Awesome", LabelId(0)),
                Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
                Set.empty,
                plannedOrder,
                IndexType.RANGE,
                supportPartitionedScan = true
              ),
              Map(v"$functionName(n.prop)" -> cachedNodeProp("n", "prop"))
            ),
            literalInt(1)
          )
        )
      )
    }

    test(
      s"$orderCapability-$functionName: should plan aggregation for node index scan when there is no $orderCapability"
    ) {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesValues()
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop IS NOT NULL RETURN $functionName(n.prop)"

      plan._1 should equal(
        Aggregation(
          NodeIndexScan(
            v"n",
            LabelToken("Awesome", LabelId(0)),
            Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
            Set.empty,
            IndexOrderNone,
            IndexType.RANGE,
            supportPartitionedScan = true
          ),
          Map.empty,
          Map(v"$functionName(n.prop)" -> function(functionName, cachedNodeProp("n", "prop")))
        )
      )
    }

    // Node property index seek

    test(s"$orderCapability-$functionName: should use provided node index order with range") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop)"

      plan._1 should equal(
        Optional(
          Limit(
            Projection(
              nodeIndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = _ => GetValue),
              Map(v"$functionName(n.prop)" -> cachedNodeProp("n", "prop"))
            ),
            literalInt(1)
          )
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided node index order with ORDER BY") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop) ORDER BY $functionName(n.prop) $cypherToken"

      plan._1 should equal(
        Optional(
          Limit(
            Projection(
              nodeIndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = _ => GetValue),
              Map(v"$functionName(n.prop)" -> cachedNodeProp("n", "prop"))
            ),
            literalInt(1)
          )
        )
      )
    }

    test(
      s"$orderCapability-$functionName: should use provided node index order followed by sort for ORDER BY with reverse order"
    ) {
      val (inverseOrder, inverseSortOrder) = cypherToken match {
        case "ASC"  => ("DESC", Descending)
        case "DESC" => ("ASC", Ascending)
      }

      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop) ORDER BY $functionName(n.prop) $inverseOrder"

      plan._1 should equal(
        Sort(
          Optional(
            Limit(
              Projection(
                nodeIndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = _ => GetValue),
                Map(v"$functionName(n.prop)" -> cachedNodeProp("n", "prop"))
              ),
              literalInt(1)
            )
          ),
          Seq(inverseSortOrder(v"$functionName(n.prop)"))
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided node index order with additional Limit") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop) LIMIT 2"

      plan._1 should equal(
        Limit(
          Optional(
            Limit(
              Projection(
                nodeIndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = _ => GetValue),
                Map(v"$functionName(n.prop)" -> cachedNodeProp("n", "prop"))
              ),
              literalInt(1)
            )
          ),
          literalInt(2)
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided node index order for multiple QueryGraphs") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor
          s"""MATCH (n:Awesome)
             |WHERE n.prop > 0
             |WITH $functionName(n.prop) AS agg
             |RETURN agg
             |ORDER BY agg $cypherToken""".stripMargin

      plan._1 should equal(
        Optional(
          Limit(
            Projection(
              nodeIndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = _ => GetValue),
              Map(v"agg" -> cachedNodeProp("n", "prop"))
            ),
            literalInt(1)
          )
        )
      )
    }

    test(s"$orderCapability-$functionName: cannot use provided node index order for multiple aggregations") {
      val plan =
        new givenConfig {
          indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop), count(n.prop)"

      plan._1 should equal(
        Aggregation(
          nodeIndexSeek("n:Awesome(prop > 0)", indexOrder = IndexOrderNone, getValue = _ => GetValue),
          Map.empty,
          Map(
            v"$functionName(n.prop)" -> function(functionName, cachedNodeProp("n", "prop")),
            v"count(n.prop)" -> count(cachedNodeProp("n", "prop"))
          )
        )
      )
    }

    // Relationship type scan

    test(s"$orderCapability-$functionName: should use relationship type scan order") {
      val planner = relationshipIndexMinMaxSetup.build()

      val plan = planner
        .plan(s"MATCH (n)-[r:REL]->(m) RETURN $functionName(r)")

      plan shouldEqual
        planner.planBuilder()
          .produceResults(s"`$functionName(r)`")
          .optional()
          .limit(1)
          .projection(s"r AS `$functionName(r)`")
          .relationshipTypeScan("(n)-[r:REL]->(m)", plannedOrder)
          .build()

    }

    // Relationship property index scan

    test(s"$orderCapability-$functionName: should use provided relationship index scan order") {
      val planner = relationshipIndexMinMaxSetup
        .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(s"MATCH (n)-[r:REL]->(m) WHERE r.prop IS NOT NULL RETURN $functionName(r.prop)")

      plan shouldEqual
        planner.planBuilder()
          .produceResults(s"`$functionName(r.prop)`")
          .optional()
          .limit(1)
          .projection(s"cacheR[r.prop] AS `$functionName(r.prop)`")
          .relationshipIndexOperator(
            "(n)-[r:REL(prop)]->(m)",
            getValue = _ => GetValue,
            indexOrder = plannedOrder,
            indexType = IndexType.RANGE
          )
          .build()
    }

    test(
      s"$orderCapability-$functionName: should plan aggregation for relationship index scan when there is no $orderCapability"
    ) {
      val planner = relationshipIndexMinMaxSetup
        .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1, withValues = true)
        .build()

      val plan = planner
        .plan(s"MATCH (n)-[r:REL]->(m) WHERE r.prop IS NOT NULL RETURN $functionName(r.prop)")

      plan shouldEqual
        planner.planBuilder()
          .produceResults(s"`$functionName(r.prop)`")
          .aggregation(Seq(), Seq(s"$functionName(cacheR[r.prop]) AS `$functionName(r.prop)`"))
          .relationshipIndexOperator("(n)-[r:REL(prop)]->(m)", getValue = _ => GetValue, indexType = IndexType.RANGE)
          .build()

    }

    // Relationship property index seeks

    test(s"$orderCapability-$functionName: should use provided relationship index order with range") {
      val planner = relationshipIndexMinMaxSetup
        .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(s"MATCH (n)-[r:REL]->(m) WHERE r.prop > 0 RETURN $functionName(r.prop)")

      plan shouldEqual
        planner.planBuilder()
          .produceResults(s"`$functionName(r.prop)`")
          .optional()
          .limit(1)
          .projection(s"cacheR[r.prop] AS `$functionName(r.prop)`")
          .relationshipIndexOperator(
            "(n)-[r:REL(prop > 0)]->(m)",
            getValue = _ => GetValue,
            indexOrder = plannedOrder,
            indexType = IndexType.RANGE
          )
          .build()
    }

    test(s"$orderCapability-$functionName: should use provided relationship index order with ORDER BY") {
      val planner = relationshipIndexMinMaxSetup
        .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(
          s"MATCH (n)-[r:REL]->(m) WHERE r.prop > 0 RETURN $functionName(r.prop) ORDER BY $functionName(r.prop) $cypherToken"
        )

      plan shouldEqual
        planner.planBuilder()
          .produceResults(s"`$functionName(r.prop)`")
          .optional()
          .limit(1)
          .projection(s"cacheR[r.prop] AS `$functionName(r.prop)`")
          .relationshipIndexOperator(
            "(n)-[r:REL(prop > 0)]->(m)",
            getValue = _ => GetValue,
            indexOrder = plannedOrder,
            indexType = IndexType.RANGE
          )
          .build()
    }

    test(
      s"$orderCapability-$functionName: should use provided relationship index order followed by sort for ORDER BY with reverse order"
    ) {
      val (inverseOrder, inverseSortOrder) = cypherToken match {
        case "ASC"  => ("DESC", Descending)
        case "DESC" => ("ASC", Ascending)
      }

      val planner = relationshipIndexMinMaxSetup
        .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(
          s"MATCH (n)-[r:REL]->(m) WHERE r.prop > 0 RETURN $functionName(r.prop) ORDER BY $functionName(r.prop) $inverseOrder"
        )

      plan shouldEqual
        planner.planBuilder()
          .produceResults(s"`$functionName(r.prop)`")
          .sortColumns(Seq(inverseSortOrder(v"$functionName(r.prop)")))
          .optional()
          .limit(1)
          .projection(s"cacheR[r.prop] AS `$functionName(r.prop)`")
          .relationshipIndexOperator(
            "(n)-[r:REL(prop > 0)]->(m)",
            getValue = _ => GetValue,
            indexOrder = plannedOrder,
            indexType = IndexType.RANGE
          )
          .build()
    }

    test(s"$orderCapability-$functionName: should use provided relationship index order with additional Limit") {
      val planner = relationshipIndexMinMaxSetup
        .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(s"MATCH (n)-[r:REL]->(m) WHERE r.prop > 0 RETURN $functionName(r.prop) LIMIT 2")

      plan shouldEqual
        planner.planBuilder()
          .produceResults(s"`$functionName(r.prop)`")
          .limit(2)
          .optional()
          .limit(1)
          .projection(s"cacheR[r.prop] AS `$functionName(r.prop)`")
          .relationshipIndexOperator(
            "(n)-[r:REL(prop > 0)]->(m)",
            getValue = _ => GetValue,
            indexOrder = plannedOrder,
            indexType = IndexType.RANGE
          )
          .build()
    }

    test(s"$orderCapability-$functionName: should use provided relationship index order for multiple QueryGraphs") {
      val planner = relationshipIndexMinMaxSetup
        .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(s"""MATCH (n)-[r:REL]->(m)
                 |WHERE r.prop > 0
                 |WITH $functionName(r.prop) AS agg
                 |RETURN agg
                 |ORDER BY agg $cypherToken""".stripMargin)

      plan shouldEqual
        planner.planBuilder()
          .produceResults("agg")
          .optional()
          .limit(1)
          .projection(s"cacheR[r.prop] AS agg")
          .relationshipIndexOperator(
            "(n)-[r:REL(prop > 0)]->(m)",
            getValue = _ => GetValue,
            indexOrder = plannedOrder,
            indexType = IndexType.RANGE
          )
          .build()
    }

    test(s"$orderCapability-$functionName: cannot use provided relationship index order for multiple aggregations") {
      val planner = relationshipIndexMinMaxSetup
        .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1, withValues = true, providesOrder = orderCapability)
        .build()

      val plan = planner
        .plan(s"MATCH (n)-[r:REL]->(m) WHERE r.prop > 0 RETURN $functionName(r.prop), count(r.prop)")

      plan shouldEqual
        planner.planBuilder()
          .produceResults(s"`$functionName(r.prop)`", "`count(r.prop)`")
          .aggregation(
            Seq(),
            Seq(s"$functionName(cacheR[r.prop]) AS `$functionName(r.prop)`", "count(cacheR[r.prop]) AS `count(r.prop)`")
          )
          .relationshipIndexOperator(
            "(n)-[r:REL(prop > 0)]->(m)",
            getValue = _ => GetValue,
            indexOrder = IndexOrderNone,
            indexType = IndexType.RANGE
          )
          .build()
    }

  }

  test("should mark leveragedOrder in collect with ORDER BY") {
    val (plan, _, attributes) =
      new givenConfig {
        indexOn("Awesome", "prop").providesOrder(BOTH)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' WITH n.prop AS p ORDER BY n.prop RETURN collect(p)"
    val leveragedOrders = attributes.leveragedOrders

    leveragedOrders.get(plan.id) should be(true)
  }
}
