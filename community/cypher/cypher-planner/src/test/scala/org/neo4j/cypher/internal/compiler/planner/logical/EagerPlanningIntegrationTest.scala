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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.EagerAnalysisImplementation
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.PropertyReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadDeleteConflict
import org.neo4j.cypher.internal.ir.EagernessReason.TypeReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.Unknown
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownLabelReadRemoveConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownLabelReadSetConflict
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationshipWithDynamicType
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlanAstConstructionTestSupport
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class EagerLPPlanningIntegrationTest extends EagerPlanningIntegrationTest(EagerAnalysisImplementation.LP)
class EagerIRPlanningIntegrationTest extends EagerPlanningIntegrationTest(EagerAnalysisImplementation.IR)

abstract class EagerPlanningIntegrationTest(impl: EagerAnalysisImplementation) extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with LogicalPlanAstConstructionTestSupport {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .withSetting(GraphDatabaseInternalSettings.cypher_eager_analysis_implementation, impl)
      // This makes it deterministic which plans ends up on what side of a CartesianProduct.
      .setExecutionModel(Volcano)

  /**
   * LP Eagerness analysis provides reasons in a lot of cases where IR Eagerness analysis simply returns `Unknown`.
   *
   * This method is to cater for these cases, only returning the provided reasons for impl == LP.
   */
  def lpReasons(reasons: EagernessReason*): ListSet[EagernessReason] = impl match {
    case GraphDatabaseInternalSettings.EagerAnalysisImplementation.IR => ListSet(Unknown)
    case GraphDatabaseInternalSettings.EagerAnalysisImplementation.LP => ListSet.from(reasons)
  }

  implicit class OptionallyEagerPlannerBuilder(b: LogicalPlanBuilder) {

    def irEager(reasons: ListSet[EagernessReason] = ListSet(Unknown)): LogicalPlanBuilder = impl match {
      case EagerAnalysisImplementation.LP => b.resetIndent()
      case EagerAnalysisImplementation.IR => b.eager(reasons)
    }

    def lpEager(reasons: ListSet[EagernessReason] = ListSet(Unknown)): LogicalPlanBuilder = impl match {
      case EagerAnalysisImplementation.IR => b.resetIndent()
      case EagerAnalysisImplementation.LP => b.eager(reasons)
    }

  }

  test("MATCH (n)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .build()

    val query = "MATCH (n)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("MATCH (n:N)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .build()

    val query = "MATCH (n:N)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.argument("n")
        .nodeByLabelScan("n", "N")
        .build()
    )
  }

  test("MATCH (n:N {prop: 42})  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .build()

    val query = "MATCH (n:N {prop: 42})  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.argument("n")
        .filter("n.prop = 42")
        .nodeByLabelScan("n", "N")
        .build()
    )
  }

  test("MATCH (n:N) WHERE n.prop > 23  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .build()

    val query = "MATCH (n:N) WHERE n.prop > 23  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.argument("n")
        .filter("n.prop > 23")
        .nodeByLabelScan("n", "N")
        .build()
    )
  }

  test("MATCH (n)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS with index") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .addNodeIndex("N", Seq("prop"), 1, 0.01)
      .build()

    val query = "MATCH (n)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("MATCH (n:N)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS with index") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .addNodeIndex("N", Seq("prop"), 1, 0.01)
      .build()

    val query = "MATCH (n:N)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.argument("n")
        .nodeByLabelScan("n", "N")
        .build()
    )
  }

  test("MATCH (n:N {prop: 42})  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS with index") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .addNodeIndex("N", Seq("prop"), 1, 0.01)
      .build()

    val query = "MATCH (n:N {prop: 42})  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.argument("n")
        .nodeIndexOperator("n:N(prop = 42)")
        .build()
    )
  }

  test("MATCH (n:N) WHERE n.prop > 23  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS with index") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .addNodeIndex("N", Seq("prop"), 1, 0.01)
      .build()

    val query = "MATCH (n:N) WHERE n.prop > 23  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.argument("n")
        .nodeIndexOperator("n:N(prop > 23)")
        .build()
    )
  }

  test("MATCH (x) WITH x, 1 as dummy  MATCH (n)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .build()

    val query = "MATCH (x) WITH x, 1 as dummy  MATCH (n)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(ReadDeleteConflict("n"))
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(7))),
          ReadDeleteConflict("x").withConflict(Conflict(Id(3), Id(9)))
        )
    }

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .irEager(ListSet(ReadDeleteConflict("n")))
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.irEager(ListSet(ReadDeleteConflict("n")))
        .|.argument("n")
        .eager(reason)
        .apply()
        .|.allNodeScan("n", "x", "dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .projection("1 AS dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .allNodeScan("x")
        .build()
    )
  }

  test("MATCH (x) WITH x, 1 as dummy  MATCH (n:N)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .build()

    val query = "MATCH (x) WITH x, 1 as dummy  MATCH (n:N)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(ReadDeleteConflict("n"))
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(7))),
          ReadDeleteConflict("x").withConflict(Conflict(Id(3), Id(9)))
        )
    }

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .irEager(ListSet(ReadDeleteConflict("n")))
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.irEager(ListSet(ReadDeleteConflict("n")))
        .|.argument("n")
        .eager(reason)
        .apply()
        .|.nodeByLabelScan("n", "N", IndexOrderNone, "x", "dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .projection("1 AS dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .allNodeScan("x")
        .build()
    )
  }

  test("MATCH (x) WITH x, 1 as dummy  MATCH (n:N {prop: 42})  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .build()

    val query = "MATCH (x) WITH x, 1 as dummy  MATCH (n:N {prop: 42})  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(ReadDeleteConflict("n"))
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(6))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(8))),
          ReadDeleteConflict("x").withConflict(Conflict(Id(3), Id(10)))
        )
    }

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .irEager(ListSet(ReadDeleteConflict("n")))
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.irEager(ListSet(ReadDeleteConflict("n")))
        .|.argument("n")
        .eager(reason)
        .filter("n.prop = 42")
        .apply()
        .|.nodeByLabelScan("n", "N", IndexOrderNone, "x", "dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .projection("1 AS dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .allNodeScan("x")
        .build()
    )
  }

  test("MATCH (x) WITH x, 1 as dummy  MATCH (n:N) WHERE n.prop > 23  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .build()

    val query =
      "MATCH (x) WITH x, 1 as dummy  MATCH (n:N) WHERE n.prop > 23  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(ReadDeleteConflict("n"))
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(6))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(8))),
          ReadDeleteConflict("x").withConflict(Conflict(Id(3), Id(10)))
        )
    }

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .irEager(ListSet(ReadDeleteConflict("n")))
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.irEager(ListSet(ReadDeleteConflict("n")))
        .|.argument("n")
        .eager(reason)
        .filter("n.prop > 23")
        .apply()
        .|.nodeByLabelScan("n", "N", IndexOrderNone, "x", "dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .projection("1 AS dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .allNodeScan("x")
        .build()
    )
  }

  test("MATCH (x) WITH x, 1 as dummy  MATCH (n)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS with index") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .addNodeIndex("N", Seq("prop"), 1, 0.01)
      .build()

    val query = "MATCH (x) WITH x, 1 as dummy  MATCH (n)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(ReadDeleteConflict("n"))
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(7))),
          ReadDeleteConflict("x").withConflict(Conflict(Id(3), Id(9)))
        )
    }

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .irEager(ListSet(ReadDeleteConflict("n")))
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.irEager(ListSet(ReadDeleteConflict("n")))
        .|.argument("n")
        .eager(reason)
        .apply()
        .|.allNodeScan("n", "x", "dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .projection("1 AS dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .allNodeScan("x")
        .build()
    )
  }

  test("MATCH (x) WITH x, 1 as dummy  MATCH (n:N)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS with index") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .addNodeIndex("N", Seq("prop"), 1, 0.01)
      .build()

    val query = "MATCH (x) WITH x, 1 as dummy  MATCH (n:N)  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(ReadDeleteConflict("n"))
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(7))),
          ReadDeleteConflict("x").withConflict(Conflict(Id(3), Id(9)))
        )
    }

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .irEager(ListSet(ReadDeleteConflict("n")))
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.irEager(ListSet(ReadDeleteConflict("n")))
        .|.argument("n")
        .eager(reason)
        .apply()
        .|.nodeByLabelScan("n", "N", IndexOrderNone, "x", "dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .projection("1 AS dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .allNodeScan("x")
        .build()
    )
  }

  test(
    "MATCH (x) WITH x, 1 as dummy  MATCH (n:N {prop: 42})  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS with index"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .addNodeIndex("N", Seq("prop"), 1, 0.01)
      .build()

    val query = "MATCH (x) WITH x, 1 as dummy  MATCH (n:N {prop: 42})  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(ReadDeleteConflict("n"))
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(7))),
          ReadDeleteConflict("x").withConflict(Conflict(Id(3), Id(9)))
        )
    }

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .irEager(ListSet(ReadDeleteConflict("n")))
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.irEager(ListSet(ReadDeleteConflict("n")))
        .|.argument("n")
        .eager(reason)
        .apply()
        .|.nodeIndexOperator("n:N(prop = 42)", argumentIds = Set("x", "dummy"))
        .irEager(ListSet(ReadDeleteConflict("x")))
        .projection("1 AS dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .allNodeScan("x")
        .build()
    )
  }

  test(
    "MATCH (x) WITH x, 1 as dummy  MATCH (n:N) WHERE n.prop > 23  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS with index"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .addNodeIndex("N", Seq("prop"), 1, 0.01)
      .build()

    val query =
      "MATCH (x) WITH x, 1 as dummy  MATCH (n:N) WHERE n.prop > 23  CALL { WITH n DETACH DELETE n } IN TRANSACTIONS"

    val plan = planner.plan(query)

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(ReadDeleteConflict("n"))
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(7))),
          ReadDeleteConflict("x").withConflict(Conflict(Id(3), Id(9)))
        )
    }

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .irEager(ListSet(ReadDeleteConflict("n")))
        .transactionForeach(1000)
        .|.detachDeleteNode("n")
        .|.irEager(ListSet(ReadDeleteConflict("n")))
        .|.argument("n")
        .eager(reason)
        .apply()
        .|.nodeIndexOperator("n:N(prop > 23)", argumentIds = Set("x", "dummy"))
        .irEager(ListSet(ReadDeleteConflict("x")))
        .projection("1 AS dummy")
        .irEager(ListSet(ReadDeleteConflict("x")))
        .allNodeScan("x")
        .build()
    )
  }

  test(
    "MATCH (a:A), (c:C) MERGE (a)-[:BAR]->(b:B) WITH c MATCH (c) WHERE (c)-[:BAR]->() RETURN count(*)"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("C", 60)
      .setLabelCardinality("B", 50)
      .setRelationshipCardinality("()-[:BAR]->()", 10)
      .setRelationshipCardinality("(:A)-[:BAR]->()", 10)
      .setRelationshipCardinality("()-[:BAR]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[:BAR]->(:B)", 10)
      .build()

    val query =
      "MATCH (a:A), (c:C) MERGE (a)-[:BAR]->(b:B) WITH c MATCH (c) WHERE (c)-[:BAR]->() RETURN count(*)"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .filterExpression(
          HasDegreeGreaterThan(v"c", Some(relTypeName("BAR")), OUTGOING, literalInt(0))(InputPosition.NONE),
          assertIsNode("c")
        )
        .eager(lpReasons(TypeReadSetConflict(relTypeName("BAR")).withConflict(Conflict(Id(5), Id(2)))))
        .apply()
        .|.merge(
          Seq(createNode("b", "B")),
          Seq(createRelationship("anon_0", "a", "BAR", "b", OUTGOING)),
          lockNodes = Set("a")
        )
        .|.filter("b:B")
        .|.expandAll("(a)-[anon_0:BAR]->(b)")
        .|.argument("a")
        .cartesianProduct()
        .|.nodeByLabelScan("c", "C")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test(
    "MATCH (a:A), (c:C) MERGE (a)-[:BAR]->(b:B) WITH c MATCH (c) WHERE (c)-[]->() RETURN count(*)"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("C", 60)
      .setLabelCardinality("B", 50)
      .setRelationshipCardinality("()-[:BAR]->()", 10)
      .setRelationshipCardinality("(:A)-[:BAR]->()", 10)
      .setRelationshipCardinality("()-[:BAR]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[:BAR]->(:B)", 10)
      .build()

    val query =
      "MATCH (a:A), (c:C) MERGE (a)-[:BAR]->(b:B) WITH c MATCH (c) WHERE (c)-[]->() RETURN count(*)"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .filterExpression(
          HasDegreeGreaterThan(v"c", None, OUTGOING, literalInt(0))(InputPosition.NONE),
          assertIsNode("c")
        )
        .eager(lpReasons(TypeReadSetConflict(relTypeName("BAR")).withConflict(Conflict(Id(5), Id(2)))))
        .apply()
        .|.merge(
          Seq(createNode("b", "B")),
          Seq(createRelationship("anon_0", "a", "BAR", "b", OUTGOING)),
          lockNodes = Set("a")
        )
        .|.filter("b:B")
        .|.expandAll("(a)-[anon_0:BAR]->(b)")
        .|.argument("a")
        .cartesianProduct()
        .|.nodeByLabelScan("c", "C")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  // SHORTEST Tests

  case class ShortestPathParameters(
    start: String,
    end: String,
    query: String,
    singletonNodeVariables: Set[(String, String)],
    singletonRelationshipVariables: Set[(String, String)],
    selector: StatefulShortestPath.Selector,
    nfa: NFA,
    minLength: Int,
    maybeMaxLength: Option[Int]
  )

  implicit class SSPLogicalPlanBuilder(builder: LogicalPlanBuilder) {

    def statefulShortestPath(parameters: ShortestPathParameters): LogicalPlanBuilder =
      builder
        .statefulShortestPath(
          parameters.start,
          parameters.end,
          parameters.query,
          None,
          Set.empty,
          Set.empty,
          parameters.singletonNodeVariables,
          parameters.singletonRelationshipVariables,
          StatefulShortestPath.Selector.Shortest(1),
          parameters.nfa,
          ExpandAll,
          false,
          parameters.minLength,
          parameters.maybeMaxLength
        )
  }

  val `(start)-[r]->(end)`: ShortestPathParameters =
    ShortestPathParameters(
      "start",
      "end",
      "SHORTEST 1 (start)-[r]->(end)",
      Set("end" -> "end"),
      Set("r" -> "r"),
      StatefulShortestPath.Selector.Shortest(1),
      new TestNFABuilder(0, "start")
        .addTransition(0, 1, "(start)-[r]->(end)")
        .setFinalState(1)
        .build(),
      1,
      Some(1)
    )

  val `((start)((a{prop: 5})-[r:R]->(b))+(end))`: ShortestPathParameters =
    ShortestPathParameters(
      "start",
      "end",
      "SHORTEST 1 (start) ((`a`)-[`r`]->(`b`)){1, } (end)",
      Set("end" -> "end"),
      Set.empty,
      StatefulShortestPath.Selector.Shortest(1),
      new TestNFABuilder(0, "start")
        .addTransition(0, 1, "(start) (a WHERE a.prop = 5)")
        .addTransition(1, 2, "(a)-[r:R]->(b)")
        .addTransition(2, 1, "(b) (a WHERE a.prop = 5)")
        .addTransition(2, 3, "(b) (end)")
        .setFinalState(3)
        .build(),
      1,
      None
    )

  val `((start)(({prop: 5})-[r:R]->())+(end))`: ShortestPathParameters =
    ShortestPathParameters(
      "start",
      "end",
      "SHORTEST 1 (start) ((`anon_0`)-[`r`]->(`b`)){1, } (end)",
      Set("end" -> "end"),
      Set.empty,
      StatefulShortestPath.Selector.Shortest(1),
      new TestNFABuilder(0, "start")
        .addTransition(0, 1, "(start) (anon_0 WHERE anon_0.prop = 5)")
        .addTransition(1, 2, "(anon_0)-[r:R]->(b)")
        .addTransition(2, 1, "(b) (anon_0 WHERE anon_0.prop = 5)")
        .addTransition(2, 3, "(b) (end)")
        .setFinalState(3)
        .build(),
      1,
      None
    )

  val `(start)-[r:R]->(end)`: ShortestPathParameters =
    ShortestPathParameters(
      "start",
      "end",
      "SHORTEST 1 (start)-[r]->(end)",
      Set("end" -> "end"),
      Set("r" -> "r"),
      StatefulShortestPath.Selector.Shortest(1),
      new TestNFABuilder(0, "start")
        .addTransition(0, 1, "(start)-[r:R]->(end)")
        .setFinalState(1)
        .build(),
      1,
      Some(1)
    )

  test("Shortest match produces an eager when there is a relationship overlap") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[]->()", 10)
      .build()

    val query = "MATCH ANY SHORTEST (start {prop: 5})-[r]->(end) CREATE (end)-[s:S]->() RETURN end"
    val plan = planner.plan(query)

    val expectedPlan = planner.planBuilder()
      .produceResults("end")
      .create(createNode("anon_0"), createRelationship("s", "end", "S", "anon_0", OUTGOING))
      .eager(lpReasons(
        ReadCreateConflict.withConflict(Conflict(Id(1), Id(3))),
        TypeReadSetConflict(relTypeName("S")).withConflict(Conflict(Id(1), Id(3)))
      ))
      .statefulShortestPath(`(start)-[r]->(end)`)
      .filter("start.prop = 5")
      .allNodeScan("start")
      .build()

    plan should equal(expectedPlan)
  }

  test("Shortest match produces an unnecessary eager when there is no relationship overlap") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[]->()", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .build()

    val query = "MATCH ANY SHORTEST (start {prop: 5})-[r:R]->(end) CREATE (end)-[s:S]->() RETURN end"
    val plan = planner.plan(query)

    val expectedPlan = planner.planBuilder()
      .produceResults("end")
      .create(createNode("anon_0"), createRelationship("s", "end", "S", "anon_0", OUTGOING))
      // Unnecessary Eager
      .lpEager(ListSet(
        ReadCreateConflict.withConflict(Conflict(Id(1), Id(3))),
        TypeReadSetConflict(relTypeName("S")).withConflict(Conflict(Id(1), Id(3)))
      ))
      .statefulShortestPath(`(start)-[r:R]->(end)`)
      .filter("start.prop = 5")
      .allNodeScan("start")
      .build()

    plan should equal(expectedPlan)
  }

  test("Shortest match should produce an eager when there is an write/read conflict with set property") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[]->()", 10)
      .build()

    val query =
      "MATCH (a) SET a.prop = 1 WITH a MATCH ANY SHORTEST (start)-[r]->(end) WHERE start.prop = 1 RETURN end.prop2"

    val plan = planner.plan(query)
    plan should equal(
      planner.planBuilder()
        .produceResults("`end.prop2`")
        .projection("end.prop2 AS `end.prop2`")
        .statefulShortestPath(
          "start",
          "end",
          "SHORTEST 1 (start)-[r]->(end)",
          None,
          Set.empty,
          Set.empty,
          Set("end" -> "end"),
          Set("r" -> "r"),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "start")
            .addTransition(0, 1, "(start)-[r]->(end)")
            .setFinalState(1)
            .build(),
          ExpandAll,
          false,
          1,
          Some(1)
        )
        .filter("start.prop = 1")
        .apply()
        .|.allNodeScan("start", "a")
        .eager(lpReasons(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(7), Id(3)))))
        .setNodeProperty("a", "prop", "1")
        .allNodeScan("a")
        .build()
    )
  }

  test("Shortest match should produce an eager when there is an write/read conflict with set dynamic property") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[]->()", 10)
      .withSetting(GraphDatabaseInternalSettings.cypher_eager_analysis_implementation, impl)
      .build()

    val query =
      "MATCH (a) SET a[$p] = 1 WITH a MATCH ANY SHORTEST (start)-[r]->(end) WHERE start.prop = 1 RETURN end.prop2"

    val plan = planner.plan(query)
    plan should equal(
      planner.planBuilder()
        .produceResults("`end.prop2`")
        .projection("end.prop2 AS `end.prop2`")
        .statefulShortestPath(
          "start",
          "end",
          "SHORTEST 1 (start)-[r]->(end)",
          None,
          Set.empty,
          Set.empty,
          Set("end" -> "end"),
          Set("r" -> "r"),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "start")
            .addTransition(0, 1, "(start)-[r]->(end)")
            .setFinalState(1)
            .build(),
          ExpandAll,
          false,
          1,
          Some(1)
        )
        .filter("start.prop = 1")
        .apply()
        .|.allNodeScan("start", "a")
        .eager(ListSet(
          EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(7), Id(1))),
          EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(7), Id(3)))
        ))
        .setDynamicProperty("a", "$p", "1")
        .allNodeScan("a")
        .build()
    )
  }

  test("Shortest match should produce an eager when there is an write/read conflict with create relationship") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[]->()", 10)
      .withSetting(GraphDatabaseInternalSettings.cypher_eager_analysis_implementation, impl)
      .build()

    val query = "CREATE (a)-[s:S]->(b) WITH b MATCH ANY SHORTEST (start {prop: 5})-[r]->(end) RETURN end"

    val expected = planner.planBuilder()
      .produceResults("end")
      .statefulShortestPath(`(start)-[r]->(end)`)
      .lpEager(ListSet(
        ReadCreateConflict.withConflict(Conflict(Id(6), Id(1))),
        TypeReadSetConflict(relTypeName("S")).withConflict(Conflict(Id(6), Id(1)))
      ))
      .filter("start.prop = 5")
      .apply()
      .|.allNodeScan("start", "b")
      .irEager()
      .create(createNode("a"), createNode("b"), createRelationship("s", "a", "S", "b", OUTGOING))
      .argument()
      .build()

    val plan = planner.plan(query)
    plan should equal(expected)
  }

  test("Shortest match should produce an eager when there is an write/read conflict with delete") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("Label", 10)
      .setRelationshipCardinality("()-[]->()", 10)
      .withSetting(GraphDatabaseInternalSettings.cypher_eager_analysis_implementation, impl)
      .build()

    val query = "MATCH (a:Label) DELETE a WITH * MATCH ANY SHORTEST (start {prop: 5})-[r]->(end) RETURN end"

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(
          ReadDeleteConflict("a"),
          ReadDeleteConflict("end")
        )
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("end").withConflict(Conflict(Id(6), Id(1))),
          ReadDeleteConflict("start").withConflict(Conflict(Id(6), Id(4)))
        )
    }

    val expected = planner.planBuilder()
      .produceResults("end")
      .statefulShortestPath(`(start)-[r]->(end)`)
      .filter("start.prop = 5")
      .apply()
      .|.allNodeScan("start", "a")
      .eager(reason)
      .deleteNode("a")
      .nodeByLabelScan("a", "Label", IndexOrderNone)
      .build()

    val plan = planner.plan(query)
    plan should equal(expected)
  }

  test(
    "Shortest match should produce an eager when there is an overlap in a non nested pattern with relationship delete"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[]->()", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .withSetting(GraphDatabaseInternalSettings.cypher_eager_analysis_implementation, impl)
      .build()

    val query =
      "MATCH ANY SHORTEST ((start {prop: 5})-[s]-(x) ((a{prop: 5})-[r:R]->(b))+(end)) DELETE s RETURN end"

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(ReadDeleteConflict("s"))
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("s").withConflict(Conflict(Id(1), Id(3))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(1), Id(3)))
        )
    }

    val plan = planner.plan(query)
    plan should equal(
      planner.planBuilder()
        .produceResults("end")
        .irEager(ListSet(ReadDeleteConflict("end")))
        .deleteRelationship("s")
        .eager(reason)
        .statefulShortestPath(
          "start",
          "end",
          "SHORTEST 1 (start)-[s]-(x) ((`a`)-[`r`]->(`b`)){1, } (end)",
          None,
          Set.empty,
          Set.empty,
          Set("x" -> "x", "end" -> "end"),
          Set("s" -> "s"),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "start")
            .addTransition(0, 1, "(start)-[s]-(x WHERE x.prop = 5)")
            .addTransition(1, 2, "(x) (a WHERE a.prop = 5)")
            .addTransition(2, 3, "(a)-[r:R]->(b)")
            .addTransition(3, 2, "(b) (a WHERE a.prop = 5)")
            .addTransition(3, 4, "(b) (end)")
            .setFinalState(4)
            .build(),
          ExpandAll,
          false,
          2,
          None
        )
        .filter("start.prop = 5")
        .allNodeScan("start")
        .build()
    )
  }

  test("Shortest match should produce an eager when there is a property overlap") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .withSetting(GraphDatabaseInternalSettings.cypher_eager_analysis_implementation, impl)
      .build()

    val query = "MATCH ANY SHORTEST (start{prop:1})-[r:R]->(end) SET end.prop = 1 RETURN end"

    val plan = planner.plan(query)
    plan should equal(
      planner.planBuilder()
        .produceResults("end")
        .eager(lpReasons(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(0)))))
        .setNodeProperty("end", "prop", "1")
        .eager(lpReasons(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(5)))))
        .statefulShortestPath(
          "start",
          "end",
          "SHORTEST 1 (start)-[r]->(end)",
          None,
          Set.empty,
          Set.empty,
          Set("end" -> "end"),
          Set("r" -> "r"),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "start")
            .addTransition(0, 1, "(start)-[r:R]->(end)")
            .setFinalState(1)
            .build(),
          ExpandAll,
          false,
          1,
          Some(1)
        )
        .filter("start.prop = 1")
        .allNodeScan("start")
        .build()
    )
  }

  test("Shortest match should produce an eager when there is a property overlap (dynamic)") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .withSetting(GraphDatabaseInternalSettings.cypher_eager_analysis_implementation, impl)
      .build()

    val query = "MATCH ANY SHORTEST (start{prop:1})-[r:R]->(end) SET end[$p] = 1 RETURN end"

    val plan = planner.plan(query)
    plan should equal(
      planner.planBuilder()
        .produceResults("end")
        .eager(ListSet(EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(
          Id(2),
          Id(0)
        ))))
        .setDynamicProperty("end", "$p", "1")
        .eager(ListSet(EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(
          Id(2),
          Id(5)
        ))))
        .statefulShortestPath(
          "start",
          "end",
          "SHORTEST 1 (start)-[r]->(end)",
          None,
          Set.empty,
          Set.empty,
          Set("end" -> "end"),
          Set("r" -> "r"),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "start")
            .addTransition(0, 1, "(start)-[r:R]->(end)")
            .setFinalState(1)
            .build(),
          ExpandAll,
          false,
          1,
          Some(1)
        )
        .filter("start.prop = 1")
        .allNodeScan("start")
        .build()
    )
  }

  test("Shortest match should produce an eager when there is a delete overlap") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .build()

    val query = "MATCH ANY SHORTEST (start {prop: 5})-[r:R]->(end) DETACH DELETE end RETURN 1"

    val expected = planner.planBuilder()
      .produceResults("1")
      .projection("1 AS 1")
      .detachDeleteNode("end")
      // This eager is unnecessary since we are limited to one shortest
      .eager(lpReasons(
        ReadDeleteConflict("end").withConflict(Conflict(Id(2), Id(4))),
        ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(4))),
        ReadDeleteConflict("start").withConflict(Conflict(Id(2), Id(4))),
        ReadDeleteConflict("start").withConflict(Conflict(Id(2), Id(5)))
      ))
      .statefulShortestPath(`(start)-[r:R]->(end)`)
      .filter("start.prop = 5")
      .allNodeScan("start")
      .build()

    val plan = planner.plan(query)
    plan should equal(expected)
  }

  test("Shortest match should not produce an eager when there is no relationship overlap on merge") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[:T]->()", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .build()

    val query =
      "MATCH ANY SHORTEST ((start {prop: 5})(({prop: 5})-[r:R]->(b))+(end)) MERGE (start)-[t:T]-(end) RETURN end"

    val expected = planner.planBuilder()
      .produceResults("end")
      .apply()
      .|.merge(Seq(), Seq(createRelationship("t", "start", "T", "end", BOTH)), Seq(), Seq(), Set("start", "end"))
      .|.expandInto("(start)-[t:T]-(end)")
      .|.argument("start", "end")
      // This eager is unnecessary since a relationship cannot have more than one type.
      .lpEager(ListSet(
        TypeReadSetConflict(relTypeName("T")).withConflict(Conflict(Id(2), Id(6)))
      ))
      .statefulShortestPath(`((start)(({prop: 5})-[r:R]->())+(end))`)
      .filter("start.prop = 5")
      .allNodeScan("start")
      .build()

    val plan = planner.plan(query)
    plan should equal(expected)
  }

  test("Shortest match should produce an eager when there is a relationship overlap on merge") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setRelationshipCardinality("()-[:T]->()", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .build()

    val query =
      "MATCH ANY SHORTEST ((start {prop: 5})((a{prop: 5})-[r:R]->(b))+(end)) MERGE (start)-[t:R]-(end) RETURN end"

    val expected = planner.planBuilder()
      .produceResults("end")
      .apply()
      .|.merge(Seq(), Seq(createRelationship("t", "start", "R", "end", BOTH)), Seq(), Seq(), Set("start", "end"))
      .|.expandInto("(start)-[t:R]-(end)")
      .|.argument("start", "end")
      .eager(lpReasons(TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(2), Id(6)))))
      .statefulShortestPath(`((start)((a{prop: 5})-[r:R]->(b))+(end))`)
      .filter("start.prop = 5")
      .allNodeScan("start")
      .build()

    val plan = planner.plan(query)
    plan should equal(expected)
  }

  test("Shortest match produces an unnecessary eager when there is no overlap on the inner qpp relationship") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("()-[]->()", 10)
      .build()

    val query =
      "MATCH ANY SHORTEST ((start:!Label {prop: 5})((a:!Label)-[r:!R]->(b:!Label))+(end:!Label)) MERGE (start)-[t:R]-(end) RETURN end"

    val plan = planner.plan(query)
    plan should equal(
      planner.planBuilder()
        .produceResults("end")
        .apply()
        .|.merge(Seq(), Seq(createRelationship("t", "start", "R", "end", BOTH)), Seq(), Seq(), Set("start", "end"))
        .|.expandInto("(start)-[t:R]-(end)")
        .|.argument("start", "end")
        .irEager()
        .statefulShortestPath(
          "start",
          "end",
          "SHORTEST 1 (start) ((`a`)-[`r`]->(`b`)){1, } (end)",
          None,
          Set.empty,
          Set.empty,
          Set("end" -> "end"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "start")
            .addTransition(0, 1, "(start) (a WHERE NOT a:Label)")
            .addTransition(1, 2, "(a)-[r WHERE NOT r:R]->(b WHERE NOT b:Label)")
            .addTransition(2, 1, "(b) (a WHERE NOT a:Label)")
            .addTransition(2, 3, "(b) (end WHERE NOT end:Label)")
            .setFinalState(3)
            .build(),
          ExpandAll,
          false,
          1,
          None
        )
        .filter("NOT start:Label", "start.prop = 5")
        .allNodeScan("start")
        .build()
    )
  }

  test("Shortest match produces an unnecessary eager on write/read for delete when there is no overlap") {
    // We cannot find the leafPlans for variables within a SPP so we plan an eager for each found variable.
    // This is only applicable when we don't have the deleted node as an argument, then we would instead just mention the overlap on the deleted node.
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("()-[]->()", 10)
      .build()

    val query =
      "MATCH (x:Label) DELETE x WITH 1 as z MATCH ANY SHORTEST ((start:!Label {prop: 5})((a:!Label{prop: 5})-[r:R]->(b:!Label))+(end:!Label)) return end"

    val reason: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(
          ReadDeleteConflict("start"),
          ReadDeleteConflict("end"),
          ReadDeleteConflict("a"),
          ReadDeleteConflict("b")
        )
      case EagerAnalysisImplementation.LP =>
        ListSet(
          ReadDeleteConflict("start").withConflict(Conflict(Id(7), Id(4))),
          ReadDeleteConflict("end").withConflict(Conflict(Id(7), Id(1))),
          ReadDeleteConflict("a").withConflict(Conflict(Id(7), Id(1))),
          ReadDeleteConflict("b").withConflict(Conflict(Id(7), Id(1)))
        )
    }

    val plan = planner.plan(query)
    plan should equal(
      planner.planBuilder()
        .produceResults("end")
        .statefulShortestPath(
          "start",
          "end",
          "SHORTEST 1 (start) ((`a`)-[`r`]->(`b`)){1, } (end)",
          None,
          Set.empty,
          Set.empty,
          Set("end" -> "end"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "start")
            .addTransition(0, 1, "(start) (a WHERE a.prop = 5 AND NOT a:Label)")
            .addTransition(1, 2, "(a)-[r:R]->(b WHERE NOT b:Label)")
            .addTransition(2, 1, "(b) (a WHERE a.prop = 5 AND NOT a:Label)")
            .addTransition(2, 3, "(b) (end WHERE NOT end:Label)")
            .setFinalState(3)
            .build(),
          ExpandAll,
          false,
          1,
          None
        )
        .filter("NOT start:Label", "start.prop = 5")
        .apply()
        .|.allNodeScan("start", "z")
        .lpEager(reason)
        .projection("1 AS z")
        .irEager(reason)
        .deleteNode("x")
        .nodeByLabelScan("x", "Label", IndexOrderNone)
        .build()
    )
  }

  test("Relationship in get degree requires an eager operator before detach delete") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Resource", 100)
      .setRelationshipCardinality("()-[:DEPENDS_ON]->()", 50)
      .build()

    val query = """MATCH (resource:Resource)
                  |  WHERE exists((resource)-[:DEPENDS_ON]->())
                  |DETACH DELETE resource""".stripMargin

    val plan = planner.plan(query)

    val eagernessReasons: ListSet[EagernessReason] = impl match {
      case EagerAnalysisImplementation.IR =>
        ListSet(
          EagernessReason.Unknown
        )
      case EagerAnalysisImplementation.LP =>
        ListSet(
          EagernessReason.ReadDeleteConflict("anon_3").withConflict(EagernessReason.Conflict(Id(2), Id(4)))
        )
    }

    val existsPredicate =
      HasDegreeGreaterThan(v"resource", Some(relTypeName("DEPENDS_ON")), OUTGOING, literalInt(0))(InputPosition.NONE)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .detachDeleteNode("resource")
        .eager(eagernessReasons)
        .filterExpression(existsPredicate)
        .nodeByLabelScan("resource", "Resource", IndexOrderNone)
        .build()
    )
  }

  test("Relationship in get degree does not require an eager operator before a standard delete") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Resource", 100)
      .setRelationshipCardinality("()-[:DEPENDS_ON]->()", 50)
      .build()

    // Note that this is a silly query, as nodes can't be deleted if they have incoming or outgoing relationships
    val query = """MATCH (resource:Resource)
                  |  WHERE exists((resource)-[:DEPENDS_ON]->())
                  |DELETE resource""".stripMargin

    val plan = planner.plan(query)

    val existsPredicate =
      HasDegreeGreaterThan(v"resource", Some(relTypeName("DEPENDS_ON")), OUTGOING, literalInt(0))(InputPosition.NONE)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .deleteNode("resource")
        .filterExpression(existsPredicate)
        .nodeByLabelScan("resource", "Resource", IndexOrderNone)
        .build()
    )
  }

  test("should eagerize complex case of write-read-conflict with returned complete entity in UNION") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L0", 10)
      .build()

    val query =
      """OPTIONAL MATCH (var0:L0)
        |  WHERE id(var0) = $param0
        |WITH var0
        |  WHERE var0 IS NULL
        |CREATE (var1:L0 {P0: 0})
        |WITH var1
        |SET var1 += $param1
        |RETURN var1
        |UNION
        |MATCH (var1:L0)
        |  WHERE id(var1) = $param0 AND var1.P0 = $param2
        |SET var1.P0 = var1.P0 + 1
        |WITH var1
        |  WHERE var1.P0 = coalesce($param2, 0) + 1
        |SET var1 += $param1
        |RETURN var1""".stripMargin

    val plan = planner.plan(query)
    plan should equal(
      planner.planBuilder()
        .produceResults("var1")
        // LP eagerness recognizes that the conflict is with produceResult
        .lpEager(ListSet(
          EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(5), Id(0))),
          EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(14), Id(0)))
        ))
        .distinct("var1 AS var1")
        .union()
        .|.projection("var1 AS var1")
        // IR eagerness interprets the conflict to be with the last projection
        .|.irEager()
        .|.setNodePropertiesFromMap("var1", "$param1", removeOtherProps = false)
        .|.eager(lpReasons(
          EagernessReason.UnknownPropertyReadSetConflict
            .withConflict(EagernessReason.Conflict(Id(5), Id(7)))
        ))
        .|.filter("cacheN[var1.P0] = coalesce($param2, 0) + 1")
        .|.eager(lpReasons(
          EagernessReason.PropertyReadSetConflict(propName("P0"))
            .withConflict(EagernessReason.Conflict(Id(9), Id(0))),
          EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(5), Id(9))),
          EagernessReason.PropertyReadSetConflict(propName("P0")).withConflict(EagernessReason.Conflict(Id(9), Id(7)))
        ))
        .|.setNodeProperty("var1", "P0", "var1.P0 + 1")
        .|.eager(lpReasons(
          EagernessReason.UnknownPropertyReadSetConflict.withConflict(EagernessReason.Conflict(Id(5), Id(11))),
          EagernessReason.PropertyReadSetConflict(propName("P0"))
            .withConflict(EagernessReason.Conflict(Id(9), Id(11)))
        ))
        .|.filter("var1:L0", "cacheNFromStore[var1.P0] = $param2")
        .|.nodeByIdSeek("var1", Set(), "$param0")
        .projection("var1 AS var1")
        // IR eagerness interprets the conflict to be with the last projection
        .irEager()
        .setNodePropertiesFromMap("var1", "$param1", removeOtherProps = false)
        .create(createNodeWithProperties("var1", Seq("L0"), "{P0: 0}"))
        .filter("var0 IS NULL")
        .irEager()
        .optional()
        .lpEager(ListSet(
          EagernessReason.LabelReadSetConflict(labelName("L0"))
            .withConflict(EagernessReason.Conflict(Id(15), Id(20)))
        ))
        .filter("var0:L0")
        .nodeByIdSeek("var0", Set(), "$param0")
        .build()
    )
  }

  test("should eagerize simple case of write-read-conflict with returned complete entity in UNION") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L0", 10)
      .setRelationshipCardinality("()-[]->()", 20)
      .setRelationshipCardinality("(:L0)-[]->()", 8)
      .setRelationshipCardinality("()-[]->(:L0)", 8)
      .build()

    val query =
      """  CREATE (var1:L0 {P0: 0})-[:REL]->(:L0)
        |  RETURN var1
        |UNION
        |  MATCH (var1:L0)--(var0)
        |  SET var0 += $param2
        |  RETURN var1;""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("var1")
        .distinct("var1 AS var1")
        .union()
        .|.lpEager(ListSet(
          EagernessReason.UnknownPropertyReadSetConflict
            .withConflict(EagernessReason.Conflict(Id(5), Id(0)))
        ))
        .|.projection("var1 AS var1")
        .|.irEager()
        .|.setNodePropertiesFromMap("var0", "$param2", removeOtherProps = false)
        .|.expandAll("(var1)-[anon_2]-(var0)")
        .|.nodeByLabelScan("var1", "L0", IndexOrderNone)
        .projection("var1 AS var1")
        .create(
          createNodeWithProperties("var1", Seq("L0"), "{P0: 0}"),
          createNode("anon_1", "L0"),
          createRelationship("anon_0", "var1", "REL", "anon_1", OUTGOING)
        )
        .argument()
        .build()
    )
  }

  test("should eagerize subquery case of write-read-conflict with returned complete entity in UNION") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L0", 10)
      .setRelationshipCardinality("()-[]->()", 20)
      .setRelationshipCardinality("(:L0)-[]->()", 8)
      .setRelationshipCardinality("()-[]->(:L0)", 8)
      .setRelationshipCardinality("(:L0)-[]->(:L0)", 8)
      .build()

    val query =
      """CALL {
        |    CREATE (var1:L0 {P0: 0})-[:REL]->(:L0)
        |    RETURN var1
        |  UNION
        |    MATCH (var1:L0)--(var0)
        |    SET var0 += $param2
        |    RETURN var1
        |}
        |RETURN var1""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("var1")
        .irEager()
        .distinct("var1 AS var1")
        .union()
        .|.lpEager(ListSet(
          EagernessReason.UnknownPropertyReadSetConflict
            .withConflict(EagernessReason.Conflict(Id(5), Id(0)))
        ))
        .|.projection("var1 AS var1")
        .|.setNodePropertiesFromMap("var0", "$param2", removeOtherProps = false)
        .|.expandAll("(var1)-[anon_2]-(var0)")
        .|.nodeByLabelScan("var1", "L0", IndexOrderNone)
        .projection("var1 AS var1")
        .create(
          createNodeWithProperties("var1", Seq("L0"), "{P0: 0}"),
          createNode("anon_1", "L0"),
          createRelationship("anon_0", "var1", "REL", "anon_1", OUTGOING)
        )
        .argument()
        .build()
    )
  }

  test("MATCH (n:Label), (m:Label) SET n:$(\"Label\") RETURN m, n") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 50)
      .setLabelCardinality("Label2", 50)
      .build()

    val query = """MATCH (n:Label), (m:Label2) SET n:$("Label2") RETURN m, n""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("m", "n")
        .eager(ListSet(UnknownLabelReadSetConflict.withConflict(Conflict(Id(2), Id(0)))))
        .setLabels("n", Seq(), Seq("'Label2'"))
        .eager(ListSet(UnknownLabelReadSetConflict.withConflict(Conflict(Id(2), Id(5)))))
        .cartesianProduct()
        .|.nodeByLabelScan("m", "Label2", IndexOrderNone)
        .nodeByLabelScan("n", "Label", IndexOrderNone)
        .build()
    )
  }

  test("MATCH (n:Movie) SET n:$(n.genre) REMOVE n.genre RETURN n;") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 50)
      .build()

    val query = """MATCH (n:Label)
                  |SET n:$(n.genre)
                  |REMOVE n.genre
                  |RETURN n""".stripMargin

    val plan = planner.plan(query)
//Produces no eager since n is unique so no risk of overwrite.
    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .setNodeProperty("n", "genre", "NULL")
        .setLabels("n", Seq(), Seq("n.genre"))
        .nodeByLabelScan("n", "Label", IndexOrderNone)
        .build()
    )
  }

  test("MATCH (m:Label), (n) SET m: $(n.genre) REMOVE m.genre RETURN n") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 50)
      .build()

    val query = """MATCH (m:Label), (n)
                  |SET m:$(n.genre)
                  |REMOVE m.genre
                  |RETURN n""".stripMargin

    val plan = planner.plan(query)

    val expected = planner.planBuilder()
      .produceResults("n")
      .eager(ListSet(PropertyReadSetConflict(propName("genre")).withConflict(Conflict(Id(2), Id(0)))))
      .setNodeProperty("m", "genre", "NULL")
      .eager(ListSet(
        UnknownLabelReadSetConflict.withConflict(Conflict(Id(4), Id(0))),
        PropertyReadSetConflict(propName("genre")).withConflict(Conflict(Id(2), Id(4)))
      ))
      .setLabels("m", Seq(), Seq("n.genre"))
      .eager(ListSet(UnknownLabelReadSetConflict.withConflict(Conflict(Id(4), Id(7)))))
      .cartesianProduct()
      .|.nodeByLabelScan("m", "Label", IndexOrderNone)
      .allNodeScan("n")
      .build()

    plan shouldEqual expected
  }

  test("MATCH (n:Label), (m) SET n: $(toString(n.year)) REMOVE n.genre, n.year RETURN n") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 50)
      .build()

    val query = """MATCH (n:Label), (m)
                  |SET n:$(toString(n.year))
                  |REMOVE n.genre, n.year
                  |RETURN n""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .eager(ListSet(PropertyReadSetConflict(propName("year")).withConflict(Conflict(Id(2), Id(0)))))
        .setNodeProperty("n", "year", "NULL")
        .eager(ListSet(
          PropertyReadSetConflict(propName("genre")).withConflict(Conflict(Id(4), Id(0))),
          UnknownLabelReadSetConflict.withConflict(Conflict(Id(5), Id(0))),
          PropertyReadSetConflict(propName("year")).withConflict(Conflict(Id(2), Id(5)))
        ))
        .setNodeProperty("n", "genre", "NULL")
        .setLabels("n", Seq(), Seq("toString(n.year)"))
        .eager(ListSet(UnknownLabelReadSetConflict.withConflict(Conflict(Id(5), Id(8)))))
        .cartesianProduct()
        .|.nodeByLabelScan("n", "Label", IndexOrderNone)
        .allNodeScan("m")
        .build()
    )
  }

  test("Should eagerize Load CSV with dynamic labels") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 50)
      .build()

    val query = """LOAD CSV WITH HEADERS FROM 'file:///artists-with-headers.csv' AS line
                  |CREATE (n {name: line.Name})
                  |SET n:$(line.label)
                  |RETURN n""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .eager(ListSet(UnknownLabelReadSetConflict.withConflict(Conflict(Id(2), Id(0)))))
        .setLabels("n", Seq(), Seq("line.label"))
        .create(createNodeWithProperties("n", Seq(), "{name: line.Name}"))
        .loadCSV("'file:///artists-with-headers.csv'", "line", HasHeaders, None)
        .argument()
        .build()
    )
  }

  test("Should eagerize nested COLLECT with dynamic labels - SET") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 50)
      .build()

    val query = """MATCH (p), (n:Person)
                  |SET p:$(COLLECT { UNWIND ["Person", "READ_ONLY"] AS strings RETURN strings})
                  |RETURN p""".stripMargin

    val plan = planner.plan(query)

    val solvedExpr =
      """COLLECT { UNWIND ["Person", "READ_ONLY"] AS strings
        |RETURN strings AS strings }""".stripMargin

    val nestedPlan = planner.subPlanBuilder()
      .unwind("['Person', 'READ_ONLY'] AS strings")
      .argument()
      .build()

    val collectExpr = nestedCollectExpr(
      plan = nestedPlan,
      projection = "strings",
      solvedExpressionAsString = solvedExpr
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("p")
        .eager(ListSet(UnknownLabelReadSetConflict.withConflict(Conflict(Id(2), Id(0)))))
        .setDynamicLabelsWithExpression("p", Set(collectExpr))
        .eager(ListSet(UnknownLabelReadSetConflict.withConflict(Conflict(Id(2), Id(7)))))
        .cartesianProduct()
        .|.nodeByLabelScan("n", "Person", IndexOrderNone)
        .allNodeScan("p")
        .build()
    )
  }

  test("Should eagerize nested COLLECT with dynamic labels - REMOVE") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 50)
      .build()

    val query = """MATCH (p), (n:Person)
                  |REMOVE p:$(COLLECT { UNWIND ["Person", "READ_ONLY"] AS strings RETURN strings})
                  |RETURN p""".stripMargin

    val plan = planner.plan(query)

    val solvedExpr =
      """COLLECT { UNWIND ["Person", "READ_ONLY"] AS strings
        |RETURN strings AS strings }""".stripMargin

    val nestedPlan = planner.subPlanBuilder()
      .unwind("['Person', 'READ_ONLY'] AS strings")
      .argument()
      .build()

    val collectExpr = nestedCollectExpr(
      plan = nestedPlan,
      projection = "strings",
      solvedExpressionAsString = solvedExpr
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("p")
        .eager(ListSet(UnknownLabelReadRemoveConflict.withConflict(Conflict(Id(2), Id(0)))))
        .removeDynamicLabelsWithExpressions("p", Set(collectExpr))
        .eager(ListSet(UnknownLabelReadRemoveConflict.withConflict(Conflict(Id(2), Id(7)))))
        .cartesianProduct()
        .|.nodeByLabelScan("n", "Person", IndexOrderNone)
        .allNodeScan("p")
        .build()
    )
  }

  test("Should eagerize removal of dynamic labels list from COLLECT") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 50)
      .build()

    val query = """WITH COLLECT { UNWIND range(0,3) AS id RETURN id} as labels
                  |MATCH (n), (m)
                  |REMOVE n:$(labels)
                  |RETURN labels(m) AS leftoverLabels""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("leftoverLabels")
        .projection("labels(m) AS leftoverLabels")
        .eager(ListSet(UnknownLabelReadRemoveConflict.withConflict(Conflict(Id(3), Id(1)))))
        .removeLabels("n", Seq(), Seq("labels"))
        .apply()
        .|.cartesianProduct()
        .|.|.allNodeScan("m", "labels")
        .|.allNodeScan("n", "labels")
        .rollUpApply("labels", "id")
        .|.unwind("range(0, 3) AS id")
        .|.argument()
        .argument()
        .build()
    )
  }

  test("Should not eagerize Merge with dynamic labels since merge is distinct") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 50)
      .build()
    // Node is distinct so no eager is necessary.
    val query = """WITH "NewLabel" AS label, "OldLabel" AS label2
                  |MERGE (gem:Label {name: "Gem"})
                  |ON CREATE
                  |  SET gem:$(label)
                  |ON MATCH
                  |  SET gem:$(label2)
                  |RETURN labels(gem) AS labels
                  |""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("labels")
        .projection("labels(gem) AS labels")
        .apply()
        .|.merge(
          Seq(createNodeWithProperties("gem", Seq("Label"), "{name: 'Gem'}")),
          Seq(),
          Seq(setLabel("gem", Seq(), Seq("label2"))),
          Seq(setLabel("gem", Seq(), Seq("label"))),
          Set()
        )
        .|.filter("gem.name = 'Gem'")
        .|.nodeByLabelScan("gem", "Label", IndexOrderNone, "label", "label2")
        .projection("'NewLabel' AS label", "'OldLabel' AS label2")
        .argument()
        .build()
    )
  }

  test("eagerness should be consistent with old call subquery syntax") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 10)
      .build()

    val query = """MATCH (s:Person {name: 'me'})
                  |CALL (s) {
                  |   SET s.seen = coalesce(s.seen + 1,1)
                  |   RETURN s.seen AS result
                  |}
                  |RETURN result""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("result")
        .apply()
        .|.projection("s.seen AS result")
        .|.eager(lpReasons(PropertyReadSetConflict(propName("seen")).withConflict(Conflict(Id(4), Id(2)))))
        .|.setNodeProperty("s", "seen", "coalesce(s.seen + 1, 1)")
        .|.argument("s")
        .filter("s.name = 'me'")
        .nodeByLabelScan("s", "Person", IndexOrderNone)
        .build()
    )
  }

  test("eagerness should handle matching on dynamic labels - Create overlap") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("C", 10)
      .build()

    val query = """WITH ["A", "B"] as labels
                  |MATCH (n:$any(labels))
                  |CREATE (:Z)
                  |RETURN n""".stripMargin

    val plan = planner.plan(query)

    val expression = hasAnyDynamicLabel(varFor("n"), varFor("labels"))

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .create(createNode("anon_0", "Z"))
        .eager(lpReasons(LabelReadSetConflict(labelName("Z")).withConflict(Conflict(Id(1), Id(5)))))
        .filterExpression(expression)
        .apply()
        .|.allNodeScan("n", "labels")
        .projection("['A', 'B'] AS labels")
        .argument()
        .build()
    )
  }

  test("eagerness should handle matching on dynamic labels - Set overlap") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Z", 10)
      .build()

    val expression =
      not(hasDynamicLabels(varFor("n"), ListLiteral(List(StringLiteral("Z")(pos.withInputLength(1))))(pos)))

    val query = """WITH ["A", "B"] as labels
                  |MATCH (n:!$(["Z"])), (m:!Z)
                  |Set m:Z
                  |RETURN m""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("m")
        .eager(ListSet(LabelReadSetConflict(labelName("Z")).withConflict(Conflict(Id(2), Id(0)))))
        .setLabels("m", Seq("Z"), Seq())
        .eager(ListSet(
          LabelReadSetConflict(labelName("Z")).withConflict(Conflict(Id(2), Id(6))),
          LabelReadSetConflict(labelName("Z")).withConflict(Conflict(Id(2), Id(8)))
        ))
        .apply()
        .|.cartesianProduct()
        .|.|.filter("NOT m:Z")
        .|.|.allNodeScan("m", "labels")
        .|.filterExpression(expression)
        .|.allNodeScan("n", "labels")
        .projection("['A', 'B'] AS labels")
        .argument()
        .build()
    )
  }

  test("eagerness should handle matching on Dynamic Labels - Delete overlap") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Z", 10)
      .setRelationshipCardinality("()-[]->()", 10)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("C", 10)
      .build()

    val query = """WITH ["A", "B"] as types
                  |MATCH (n:$([])), (m:!%)
                  |DELETE m
                  |RETURN n""".stripMargin

    val plan = planner.plan(query)

    val dynExpr = hasDynamicLabels(varFor("n"), ListLiteral(List.empty)(pos))

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(0)))))
        .deleteNode("m")
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(6))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(7))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(8))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(9)))
        ))
        .apply()
        .|.cartesianProduct()
        .|.|.filterExpression(dynExpr)
        .|.|.allNodeScan("n", "types")
        .|.filter("NOT m:%")
        .|.allNodeScan("m", "types")
        .projection("['A', 'B'] AS types")
        .argument()
        .build()
    )
  }

  test("eagerness should handle matching on Dynamic Types - Create overlap") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Z", 10)
      .setRelationshipCardinality("()-[]->()", 10)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("C", 10)
      .build()

    val query = """WITH ["A", "B"] as types
                  |MATCH (n:!A)-[r1:$(["Z"])&!B]->(m:!C)
                  |CREATE (a:A)-[r2:B]->(b:C)
                  |RETURN r1""".stripMargin

    val plan = planner.plan(query)

    val dynExpr = hasDynamicType(varFor("r1"), ListLiteral(List(StringLiteral("Z")(pos.withInputLength(1))))(pos))
    val notExpr = not(hasTypes("r1", "B"))
    val andsExpr = andsReorderable("NOT n:A", "NOT m:C")

    plan should equal(
      planner.planBuilder()
        .produceResults("r1")
        .create(createNode("a", "A"), createNode("b", "C"), createRelationship("r2", "a", "B", "b", OUTGOING))
        .eager(ListSet(
          LabelReadSetConflict(labelName("C")).withConflict(Conflict(Id(1), Id(5))),
          LabelReadSetConflict(labelName("A")).withConflict(Conflict(Id(1), Id(5)))
        ))
        .filterExpression(dynExpr, notExpr, andsExpr)
        .apply()
        .|.allRelationshipsScan("(n)-[r1]->(m)", "types")
        .projection("['A', 'B'] AS types")
        .argument()
        .build()
    )
  }

  test("insert an eager between creating a node with a dynamic label and reading nodes") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """CREATE (:$($label))
        |WITH *
        |MATCH (n:Account)
        |RETURN n""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("n")
      .apply()
      .|.nodeByLabelScan("n", "Account", IndexOrderNone)
      .eager(ListSet(EagernessReason.ReadCreateConflict.withConflict(EagernessReason.Conflict(Id(4), Id(2)))))
      .create(createNodeFull("anon_0", dynamicLabels = Seq("$label")))
      .argument()
      .build()
  }

  test("insert an eager between reading nodes and creating a node with a dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """UNWIND [1] AS one
        |MATCH (n:Account)
        |CREATE (:$($label))""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .create(createNodeFull("anon_0", dynamicLabels = Seq("$label")))
      .eager(ListSet(EagernessReason.ReadCreateConflict.withConflict(EagernessReason.Conflict(Id(2), Id(5)))))
      .apply()
      .|.nodeByLabelScan("n", "Account", "one")
      .unwind("[1] AS one")
      .argument()
      .build()
  }

  test("insert an eager between reading a relationship and creating a relationship with a dynamic type") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("A", 1)
        .setRelationshipCardinality("()-[]->()", 1)
        .build()

    val query =
      """CREATE (:A)-[:$("Foo")]->(:A)
        |WITH *
        |MATCH (:!A)-[r:!Foo]->(:!A)
        |RETURN r
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("r")
      .filterExpression(
        not(hasTypes("r", "Foo")),
        andsReorderableAst(not(hasLabels("anon_3", "A")), not(hasLabels("anon_4", "A")))
      )
      .apply()
      .|.allRelationshipsScan("(anon_3)-[r]->(anon_4)")
      .eager(ListSet(EagernessReason.ReadCreateConflict.withConflict(EagernessReason.Conflict(Id(5), Id(3)))))
      .create(
        createNodeFull("anon_0", labels = Seq("A")),
        createNodeFull("anon_2", labels = Seq("A")),
        createRelationshipWithDynamicType("anon_1", "anon_0", "'Foo'", "anon_2", OUTGOING)
      )
      .argument()
      .build()
  }

  test("insert an eager between reading nodes and merging a node with a dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """UNWIND [1] AS one
        |MATCH (n:Account)
        |MERGE (:$($label))""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .apply()
      .|.merge(Seq(createNodeFull("anon_0", dynamicLabels = Seq("$label"))))
      .|.filterExpression(hasDynamicLabels(varFor("anon_0"), parameter("label", CTAny)))
      .|.allNodeScan("anon_0")
      .eager(ListSet(EagernessReason.ReadCreateConflict.withConflict(EagernessReason.Conflict(Id(3), Id(8)))))
      .apply()
      .|.nodeByLabelScan("n", "Account", IndexOrderNone, "one")
      .unwind("[1] AS one")
      .argument()
      .build()
  }

  test("insert an eager between reading relationships and merging a relationship with a dynamic type") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(1000)
        .setLabelCardinality("A", 100)
        .setLabelCardinality("B", 100)
        .setRelationshipCardinality("()-[]->()", 1000)
        .setRelationshipCardinality("(:A)-[]->()", 250)
        .setRelationshipCardinality("()-[]->(:A)", 250)
        .setRelationshipCardinality("(:A)-[]->(:A)", 100)
        .setRelationshipCardinality("(:B)-[]->()", 250)
        .setRelationshipCardinality("()-[]->(:B)", 250)
        .setRelationshipCardinality("(:B)-[]->(:B)", 100)
        .build()

    val query =
      """MERGE (:A)-[:$("Foo")]->(:A)
        |WITH *
        |MATCH (:B)-[r:!Foo]->(:B)
        |RETURN r
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("r")
      .filterExpression(not(hasTypes("r", "Foo")), hasLabels("anon_4", "B"))
      .expandAll("(anon_3)-[r]->(anon_4)")
      .apply()
      .|.nodeByLabelScan("anon_3", "B", IndexOrderNone, "anon_0", "anon_2", "anon_1")
      .eager(ListSet(EagernessReason.ReadCreateConflict.withConflict(EagernessReason.Conflict(Id(6), Id(2)))))
      .merge(
        nodes = Seq(createNodeFull("anon_0", labels = Seq("A")), createNodeFull("anon_2", labels = Seq("A"))),
        relationships = Seq(createRelationshipWithDynamicType("anon_1", "anon_0", "'Foo'", "anon_2", OUTGOING))
      )
      .filterExpression(
        hasDynamicType(varFor("anon_1"), literal("Foo")),
        hasLabels("anon_2", "A")
      )
      .expandAll("(anon_0)-[anon_1]->(anon_2)")
      .nodeByLabelScan("anon_0", "A", IndexOrderNone)
      .build()
  }

  test("Eager should be inserted between MATCH and FOREACH REMOVE with dynamic label") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("100", 50)
      .build()

    val query =
      """
        |MATCH (), (n:`100`)
        |FOREACH (i IN range(1, 5) | REMOVE n:$(toString(i)))
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .foreach(
        variable = "i",
        expression = "range(1, 5)",
        mutations = Seq(removeLabel(node = "n", staticLabels = Seq(), dynamicLabelExpressions = Seq("toString(i)")))
      )
      .eager(
        ListSet(EagernessReason.UnknownLabelReadRemoveConflict.withConflict(EagernessReason.Conflict(Id(2), Id(5))))
      )
      .cartesianProduct()
      .|.nodeByLabelScan(node = "n", label = "100")
      .allNodeScan("anon_0")
      .build()
  }

  test("Eager should be inserted between FOREACH REMOVE with dynamic label and MATCH") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("100", 50)
      .build()

    val query =
      """
        |WITH [i IN range(1, 5) | toString(i)] AS numericLabels
        |MATCH (n:$any(numericLabels))
        |FOREACH (l IN numericLabels | REMOVE n:$(l))
        |WITH *
        |MATCH ()
        |MATCH (m:$all(numericLabels))
        |RETURN count(m) AS shouldBeZero
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner
      .subPlanBuilder()
      .aggregation(groupingExpressions = Seq(), aggregationExpression = Seq("count(m) AS shouldBeZero"))
      .apply()
      .|.cartesianProduct()
      .|.|.allNodeScan(node = "anon_0", "n", "numericLabels")
      .|.filterExpression(hasDynamicLabels(varFor("m"), varFor("numericLabels")))
      .|.allNodeScan(node = "m", "n", "numericLabels")
      .eager(
        ListSet(EagernessReason.UnknownLabelReadRemoveConflict.withConflict(EagernessReason.Conflict(Id(8), Id(5))))
      )
      .foreach(
        variable = "l",
        expression = "numericLabels",
        mutations = Seq(removeLabel(node = "n", staticLabels = Seq(), dynamicLabelExpressions = Seq("l")))
      )
      .filterExpression(hasAnyDynamicLabel(varFor("n"), varFor("numericLabels")))
      .apply()
      .|.allNodeScan(node = "n", "numericLabels")
      .projection("[i IN range(1, 5) | toString(i)] AS numericLabels")
      .argument()
      .build()
  }

  test("Eager should be inserted between MATCH and MERGE ON CREATE with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """UNWIND [1] AS one
        |MATCH (account:Account)
        |MERGE (n)
        |ON CREATE
        |  SET n:$($label)""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .apply()
      .|.merge(nodes = Seq(createNodeFull("n")), onCreate = Seq(setLabel("n", Seq(), Seq("$label"))))
      .|.allNodeScan("n")
      .eager(ListSet(EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(3), Id(7)))))
      .apply()
      .|.nodeByLabelScan("account", "Account", IndexOrderNone, "one")
      .unwind("[1] AS one")
      .argument()
      .build()
  }

  test("Eager should be inserted between MATCH and MERGE ON MATCH with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Account", 10)
        .build()

    val query =
      """UNWIND [1] AS one
        |MATCH (account:Account)
        |MERGE (n)
        |ON MATCH
        |  SET n:$($label)""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .apply()
      .|.merge(nodes = Seq(createNodeFull("n")), onMatch = Seq(setLabel("n", Seq(), Seq("$label"))))
      .|.allNodeScan("n")
      .eager(ListSet(EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(3), Id(7)))))
      .apply()
      .|.nodeByLabelScan("account", "Account", IndexOrderNone, "one")
      .unwind("[1] AS one")
      .argument()
      .build()
  }

  test("Eager should be inserted between MERGE ON CREATE and MATCH with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("Account", 50)
        .setLabelCardinality("Person", 50)
        .build()

    val query =
      """MERGE (n:Account)
        |ON CREATE
        |  SET n:$($label)
        |WITH *
        |MATCH (account:Person)
        |RETURN account""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("account")
      .apply()
      .|.nodeByLabelScan(node = "account", label = "Person", "n")
      .eager(
        ListSet(
          UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(0))),
          EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(2)))
        )
      )
      .merge(
        nodes = Seq(createNodeFull(node = "n", labels = Seq("Account"))),
        onCreate = Seq(setLabel(node = "n", staticLabels = Seq(), dynamicLabelExpressions = Seq("$label")))
      )
      .nodeByLabelScan(node = "n", label = "Account")
      .build()
  }

  test("Eager should be inserted between MERGE ON MATCH and MATCH with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("Account", 50)
        .setLabelCardinality("Person", 50)
        .build()

    val query =
      """MERGE (n:Account)
        |ON MATCH
        |  SET n:$($label)
        |WITH *
        |MATCH (account:Person)
        |RETURN account""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("account")
      .apply()
      .|.nodeByLabelScan(node = "account", label = "Person", "n")
      .eager(
        ListSet(
          UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(0))),
          EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(2)))
        )
      )
      .merge(
        nodes = Seq(createNodeFull(node = "n", labels = Seq("Account"))),
        onMatch = Seq(setLabel(node = "n", staticLabels = Seq(), dynamicLabelExpressions = Seq("$label")))
      )
      .nodeByLabelScan(node = "n", label = "Account")
      .build()
  }

  test("Eager should be inserted between MATCH and subquery containing MERGE ON CREATE with dynamic label") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("A", 20)
        .setLabelCardinality("B", 20)
        .build()

    val query =
      """MATCH (), (n:A)
        |CALL (n) {
        |  MERGE (m:B)
        |  ON CREATE
        |    SET m:$(labels(n))
        |  RETURN m.p AS mp
        |}
        |RETURN n.p AS np, mp""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("np", "mp")
      .projection("n.p AS np")
      .apply()
      .|.projection("m.p AS mp")
      .|.merge(
        nodes = Seq(createNodeFull(node = "m", labels = Seq("B"))),
        onCreate = Seq(setLabel(node = "m", staticLabels = Seq(), dynamicLabelExpressions = Seq("labels(n)")))
      )
      .|.nodeByLabelScan(node = "m", label = "B", "n")
      .eager(ListSet(EagernessReason.UnknownLabelReadSetConflict.withConflict(EagernessReason.Conflict(Id(4), Id(8)))))
      .cartesianProduct()
      .|.nodeByLabelScan(node = "n", label = "A")
      .allNodeScan("anon_0")
      .build()
  }

  test("An eager should be inserted if the return type is a map since it could contain a reference to a node") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(3)
        .setLabelCardinality("B", 3)
        .build()

    val query =
      """
        |UNWIND range(1, 3) as i
        |MATCH (n:B)
        |SET n.prop = i
        |RETURN {node: n} as n
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .lpEager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(0)))))
        .projection("{node: n} AS n")
        .setNodeProperty("n", "prop", "i")
        .apply()
        .|.nodeByLabelScan("n", "B", IndexOrderNone, "i")
        .unwind("range(1, 3) AS i")
        .argument()
        .build()
    )
  }

  test(
    "An eager should be inserted if the return type is a map containing a Collect subquery since it could contain a reference to a node"
  ) {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(3)
        .setLabelCardinality("B", 3)
        .build()

    val query =
      """
        |UNWIND range(1, 3) as i
        |MATCH (n:B)
        |SET n.prop = i
        |RETURN {node: COLLECT{MATCH (c) Return (c)}} as n
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .lpEager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(5), Id(0)))))
        .projection("{node: anon_0} AS n")
        .rollUpApply("anon_0", "c")
        .|.allNodeScan("c")
        .irEager()
        .setNodeProperty("n", "prop", "i")
        .apply()
        .|.nodeByLabelScan("n", "B", IndexOrderNone, "i")
        .unwind("range(1, 3) AS i")
        .argument()
        .build()
    )
  }

  test("An eager should be inserted if the return type is a List since it could contain a reference to a node") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(3)
        .setLabelCardinality("B", 3)
        .build()

    val query =
      """
        |UNWIND range(1, 3) as i
        |MATCH (n:B)
        |SET n.prop = i
        |RETURN [n] as z
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .lpEager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(0)))))
        .projection("[n] AS z")
        .setNodeProperty("n", "prop", "i")
        .apply()
        .|.nodeByLabelScan("n", "B", IndexOrderNone, "i")
        .unwind("range(1, 3) AS i")
        .argument()
        .build()
    )
  }

  test(
    "An eager should be inserted if the return type is a List within a Map since it could contain a reference to a node"
  ) {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(3)
        .setLabelCardinality("B", 3)
        .build()

    val query =
      """
        |UNWIND range(1, 3) as i
        |MATCH (n:B)
        |SET n.prop = i
        |RETURN {Nodes: [n]} as z
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .lpEager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(0)))))
        .projection("{Nodes: [n]} AS z")
        .setNodeProperty("n", "prop", "i")
        .apply()
        .|.nodeByLabelScan("n", "B", IndexOrderNone, "i")
        .unwind("range(1, 3) AS i")
        .argument()
        .build()
    )
  }

  ignore("An eager should not be inserted if the return type is a List which references a non overlapping node") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(3)
        .setLabelCardinality("B", 3)
        .build()

    val query =
      """
        |UNWIND range(1, 3) as i
        |MATCH (n:B), (m:!B)
        |SET n.prop = i
        |RETURN [m] as z
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .projection("[m] AS z")
        .setNodeProperty("n", "prop", "i")
        .apply()
        .|.nodeByLabelScan("n", "B", IndexOrderNone, "i")
        .unwind("range(1, 3) AS i")
        .argument()
        .build()
    )
  }

  test("An eager should be inserted if the return type is a map since it could contain a reference to a relationship") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(1)
        .setRelationshipCardinality("()-[:R]->()", 1)
        .build()

    val query =
      """
        |UNWIND range(1, 3) as i
        |MATCH ()-[r:R]->()
        |SET r.prop = i
        |RETURN {relationship: r} as r
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .lpEager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(0)))))
        .projection("{relationship: r} AS r")
        .setRelationshipProperty("r", "prop", "i")
        .apply()
        .|.relationshipTypeScan("(anon_0)-[r:R]->(anon_1)", IndexOrderNone, "i")
        .unwind("range(1, 3) AS i")
        .argument()
        .build()
    )
  }

  test(
    "An eager should be inserted if the return type is a path since it could contain a reference to a relationship"
  ) {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(1)
        .setLabelCardinality("B", 1)
        .build()

    val query =
      """
        |UNWIND range(1, 3) as i
        |MATCH p = (n:B)
        |SET n.prop = i
        |RETURN p
        |""".stripMargin

    val pathExpr = PathExpressionBuilder
      .node("n")
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .lpEager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(0)))))
        .projection(Map("p" -> pathExpr))
        .setNodeProperty("n", "prop", "i")
        .apply()
        .|.nodeByLabelScan("n", "B", IndexOrderNone, "i")
        .unwind("range(1, 3) AS i")
        .argument()
        .build()
    )
  }

  test("An eager should NOT be inserted before produce result if it only contains a Property Reference") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 10)
      .build()

    val query = """MATCH (s:Person {name: 'me'})
                  |CALL (s) {
                  |   SET s.seen = coalesce(s.seen + 1,1)
                  |   RETURN [s.seen] AS result
                  |}
                  |RETURN result""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("result")
        .apply()
        .|.projection("[s.seen] AS result")
        // This eager is expected, the eager we don't want is right before the produce result
        .|.irEager()
        .|.lpEager(ListSet(PropertyReadSetConflict(propName("seen")).withConflict(Conflict(Id(4), Id(2)))))
        .|.setNodeProperty("s", "seen", "coalesce(s.seen + 1, 1)")
        .|.argument("s")
        .filter("s.name = 'me'")
        .nodeByLabelScan("s", "Person", IndexOrderNone)
        .build()
    )
  }

  test("An eager should NOT be inserted before produce result if it only contains a OperatorExpression") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 10)
      .build()

    val query = """MATCH (s:Person {name: 'me'})
                  |CALL (s) {
                  |   SET s.seen = coalesce(s.seen + 1,1)
                  |   RETURN [s IS NOT NULL] AS result
                  |}
                  |RETURN result""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("result")
        .apply()
        .|.projection("[s IS NOT NULL] AS result")
        .|.setNodeProperty("s", "seen", "coalesce(s.seen + 1, 1)")
        .|.argument("s")
        .filter("s.name = 'me'")
        .nodeByLabelScan("s", "Person", IndexOrderNone)
        .build()
    )
  }
}
