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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.OptionValues

class FindShortestPathsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport with OptionValues {

  test("finds shortest paths") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (a), (b), shortestPath((a)-[r]->(b)) RETURN b").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .shortestPath("(a)-[r*1..1]->(b)", pathName = Some("anon_0"))
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
  }

  test("find shortest path with length predicate and WITH should not plan fallback") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan =
      cfg.plan("MATCH (a), (b), p = shortestPath((a)-[r]->(b)) WITH p WHERE length(p) > 1 RETURN p").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("length(p) > 1")
      .shortestPath("(a)-[r*1..1]->(b)", pathName = Some("p"))
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
  }

  test("finds all shortest paths") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (a), (b), allShortestPaths((a)-[r]->(b)) RETURN b").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .shortestPath("(a)-[r*1..1]->(b)", pathName = Some("anon_0"), all = true)
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
  }

  test("find shortest paths on top of hash joins") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setLabelCardinality("X", 100)
      .setRelationshipCardinality("()-[]->()", 99999)
      .setRelationshipCardinality("()-[]->(:X)", 100)
      .build()

    val plan =
      cfg.plan("MATCH (a:X)<-[r1]-(b)-[r2]->(c:X), p = shortestPath((a)-[r]->(c)) RETURN p").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .shortestPath("(a)-[r*1..1]->(c)", pathName = Some("p"))
      .filter("not r2 = r1")
      .nodeHashJoin("b")
      .|.expandAll("(c)<-[r2]-(b)")
      .|.nodeByLabelScan("c", "X")
      .expandAll("(a)<-[r1]-(b)")
      .nodeByLabelScan("a", "X")
      .build()
  }

  test("Inline predicates in fallback plan of var expand during shortest path") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 99999)
      .build()
    val query =
      """
        |MATCH p=shortestPath((a)-[*]-(b))
        |WHERE all(r IN relationships(p) WHERE r.prop = 10)
        |AND length(p) > 4
        |RETURN p
        |""".stripMargin
    val plan = cfg.plan(query).stripProduceResults
    val expected = cfg.subPlanBuilder()
      .antiConditionalApply("p")
      .|.top(1, "anon_1 ASC")
      .|.projection("length(p) AS anon_1")
      .|.filter("length(p) > 4")
      .|.projection(Map("p" -> PathExpression(NodePathStep(
        v"a",
        MultiRelationshipPathStep(v"anon_0", BOTH, Some(v"b"), NilPathStep()(pos))(pos)
      )(pos))(pos)))
      .|.expand(
        "(a)-[anon_0*1..]-(b)",
        expandMode = ExpandInto,
        relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] = 10"))
      )
      .|.argument("a", "b")
      .apply()
      .|.optional("a", "b")
      .|.shortestPath(
        "(a)-[anon_0*1..]-(b)",
        pathName = Some("p"),
        relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] = 10")),
        pathPredicates = Seq("length(p) > 4"),
        withFallback = true
      )
      .|.argument("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
    plan shouldEqual expected
  }

  test("finds shortest path do fallback") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 100)
      .build()
    val plan =
      cfg.plan("MATCH (a), (b), p=shortestPath((a)-[r*]->(b)) WHERE length(p) > 1 RETURN b").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .antiConditionalApply("p")
      .|.top(1, "anon_0 ASC")
      .|.projection("length(p) AS anon_0")
      .|.filter("length(p) > 1")
      .|.projection(
        Map("p" ->
          PathExpression(
            NodePathStep(
              v"a",
              multiRelationshipPathStep("r", OUTGOING, "b")
            )(pos)
          )(pos))
      )
      .|.expand("(a)-[r*]->(b)", expandMode = ExpandInto, projectedDir = OUTGOING)
      .|.argument("a", "b")
      .apply()
      .|.optional("a", "b")
      .|.shortestPath("(a)-[r*]->(b)", Some("p"), pathPredicates = Seq("length(p) > 1"), withFallback = true)
      .|.argument("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
  }

  test("finds shortest path do fallback with per step relationship predicates from relationship list") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 100)
      .build()
    val plan =
      cfg.plan(
        """
          |MATCH (a), (b), p=shortestPath((a)-[rs*]->(b))
          |WHERE length(p) > 1
          |AND all(r IN rs WHERE r.prop > 1)
          |RETURN b"""
          .stripMargin
      ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .antiConditionalApply("p")
      .|.top(1, "anon_0 ASC")
      .|.projection("length(p) AS anon_0")
      .|.filter("length(p) > 1")
      .|.projection(Map("p" -> outgoingPathExpression("a", "rs", "b")))
      .|.expand(
        "(a)-[rs*]->(b)",
        expandMode = ExpandInto,
        projectedDir = OUTGOING,
        relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] > 1"))
      )
      .|.argument("a", "b")
      .apply()
      .|.optional("a", "b")
      .|.shortestPath(
        "(a)-[rs*]->(b)",
        Some("p"),
        relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] > 1")),
        pathPredicates = Seq("length(p) > 1"),
        withFallback = true
      )
      .|.argument("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
  }

  test("finds shortest path do fallback with per step relationship predicates from path") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 100)
      .build()
    val plan =
      cfg.plan(
        """
          |MATCH (a), (b), p=shortestPath((a)-[rs*]->(b))
          |WHERE length(p) > 1
          |AND all(r IN relationships(p) WHERE r.prop > 1)
          |RETURN b"""
          .stripMargin
      ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .antiConditionalApply("p")
      .|.top(1, "anon_0 ASC")
      .|.projection("length(p) AS anon_0")
      .|.filter("length(p) > 1")
      .|.projection(Map("p" -> outgoingPathExpression("a", "rs", "b")))
      .|.expand(
        "(a)-[rs*]->(b)",
        expandMode = ExpandInto,
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] > 1")),
        projectedDir = OUTGOING
      )
      .|.argument("a", "b")
      .apply()
      .|.optional("a", "b")
      .|.shortestPath(
        "(a)-[rs*]->(b)",
        Some("p"),
        relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] > 1")),
        pathPredicates = Seq("length(p) > 1"),
        withFallback = true
      )
      .|.argument("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
  }

  test("finds shortest path do fallback with per step node predicates from path") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 100)
      .build()
    val plan =
      cfg.plan(
        """
          |MATCH (a), (b), p=shortestPath((a)-[rs*]->(b))
          |WHERE length(p) > 1
          |AND all(n IN nodes(p) WHERE n.prop > 1)
          |RETURN b"""
          .stripMargin
      ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .antiConditionalApply("p")
      .|.top(1, "anon_0 ASC")
      .|.projection("length(p) AS anon_0")
      .|.filter("length(p) > 1")
      .|.projection(Map("p" -> outgoingPathExpression("a", "rs", "b")))
      .|.expand(
        "(a)-[rs*]->(b)",
        expandMode = ExpandInto,
        nodePredicates = Seq(Predicate("n", "cacheNFromStore[n.prop] > 1")),
        projectedDir = OUTGOING
      )
      .|.argument("a", "b")
      .apply()
      .|.optional("a", "b")
      .|.shortestPath(
        "(a)-[rs*]->(b)",
        Some("p"),
        nodePredicates = Seq(Predicate("n", "cacheNFromStore[n.prop] > 1")),
        pathPredicates = Seq("length(p) > 1"),
        withFallback = true
      )
      .|.argument("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
  }

  test("should include dependencies in argument for fallback plan of var expand during shortest path") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 99999)
      .build()
    val query =
      """
        |WITH 1 AS nodeVal, 2 AS relVal, 3 AS pathVal
        |MATCH p=shortestPath((a)-[*]->(b))
        |WHERE all(r IN relationships(p) WHERE r.prop = relVal)
        |AND all(n IN nodes(p) WHERE n.prop = nodeVal)
        |AND length(p) > pathVal
        |RETURN p
        |""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("p")
        .apply()
        .|.antiConditionalApply("p")
        .|.|.top(1, "anon_1 ASC")
        .|.|.projection("length(p) AS anon_1")
        .|.|.filter("length(p) > pathVal")
        .|.|.projection(Map("p" -> outgoingPathExpression("a", "anon_0", "b")))
        .|.|.expand(
          "(a)-[anon_0*1..]->(b)",
          expandMode = ExpandInto,
          projectedDir = OUTGOING,
          nodePredicates = Seq(Predicate("n", "cacheNFromStore[n.prop] = nodeVal")),
          relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] = relVal"))
        )
        .|.|.argument("relVal", "nodeVal", "a", "pathVal", "b")
        .|.apply()
        .|.|.optional("nodeVal", "pathVal", "b", "relVal", "a")
        .|.|.shortestPath(
          "(a)-[anon_0*1..]->(b)",
          pathName = Some("p"),
          nodePredicates = Seq(Predicate("n", "cacheNFromStore[n.prop] = nodeVal")),
          relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] = relVal")),
          pathPredicates = Seq("length(p) > pathVal"),
          withFallback = true
        )
        .|.|.argument("relVal", "nodeVal", "a", "pathVal", "b")
        .|.cartesianProduct()
        .|.|.allNodeScan("b", "nodeVal", "relVal", "pathVal")
        .|.allNodeScan("a", "nodeVal", "relVal", "pathVal")
        .projection("1 AS nodeVal", "2 AS relVal", "3 AS pathVal")
        .argument()
        .build()
    )
  }

  test(
    "should include dependencies in argument for fallback plan of var expand during shortest path with relationship list"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 99999)
      .build()
    val query =
      """
        |WITH 1 AS nodeVal, 2 AS relVal, 3 AS pathVal
        |MATCH p=shortestPath((a)-[rs*]->(b))
        |WHERE all(r IN rs WHERE r.prop = relVal AND r.prop2 = size(rs))
        |AND all(n IN nodes(p) WHERE n.prop = nodeVal)
        |AND length(p) > pathVal
        |RETURN p
        |""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("p")
        .apply()
        .|.antiConditionalApply("p")
        .|.|.top(1, "anon_0 ASC")
        .|.|.projection("length(p) AS anon_0")
        .|.|.filter("length(p) > pathVal", "all(r IN rs WHERE cacheRFromStore[r.prop2] = size(rs))")
        .|.|.projection(Map("p" -> outgoingPathExpression("a", "rs", "b")))
        .|.|.expand(
          "(a)-[rs*1..]->(b)",
          expandMode = ExpandInto,
          projectedDir = OUTGOING,
          nodePredicates = Seq(Predicate("n", "cacheNFromStore[n.prop] = nodeVal")),
          relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] = relVal"))
        )
        .|.|.argument("relVal", "nodeVal", "a", "pathVal", "b")
        .|.apply()
        .|.|.optional("nodeVal", "pathVal", "b", "relVal", "a")
        .|.|.shortestPath(
          "(a)-[rs*1..]->(b)",
          pathName = Some("p"),
          nodePredicates = Seq(Predicate("n", "cacheNFromStore[n.prop] = nodeVal")),
          relationshipPredicates = Seq(Predicate("r", "cacheRFromStore[r.prop] = relVal")),
          pathPredicates = Seq("all(r IN rs WHERE cacheRFromStore[r.prop2] = size(rs))", "length(p) > pathVal"),
          withFallback = true
        )
        .|.|.argument("relVal", "nodeVal", "a", "pathVal", "b")
        .|.cartesianProduct()
        .|.|.allNodeScan("b", "nodeVal", "relVal", "pathVal")
        .|.allNodeScan("a", "nodeVal", "relVal", "pathVal")
        .projection("1 AS nodeVal", "2 AS relVal", "3 AS pathVal")
        .argument()
        .build()
    )
  }

  test("should extract predicates regardless of function name spelling") {
    val functionNames = Seq(("nodes", "relationships"), ("NODES", "RELATIONSHIPS"))
    for ((nodesF, relationshipsF) <- functionNames) withClue((nodesF, relationshipsF)) {

      val planner = plannerBuilder()
        .setAllNodesCardinality(1000)
        .setLabelCardinality("A", 100)
        .setLabelCardinality("B", 900)
        .setRelationshipCardinality("()-[]->()", 500)
        .build()

      val q =
        s"""MATCH (a:A), (b:B)
           |MATCH p = shortestPath( (a)-[r*]->(b) )
           |WHERE
           |  ALL(x IN $nodesF(p) WHERE x.prop > 123) AND
           |  ALL(y IN $relationshipsF(p) WHERE y.prop <> 'hello') AND
           |  NONE(z IN $nodesF(p) WHERE z.otherProp < 321) AND
           |  NONE(t IN $relationshipsF(p) WHERE t.otherProp IS NULL)
           |RETURN count(*) AS result""".stripMargin

      val plan = planner.plan(q).stripProduceResults

      val nodePredicates = Seq(Predicate("x", "x.prop > 123"), Predicate("z", "not z.otherProp < 321"))
      val relPredicates = Seq(Predicate("y", "not y.prop = 'hello'"), Predicate("t", "not t.otherProp IS NULL"))
      plan shouldEqual planner.subPlanBuilder()
        .aggregation(Seq(), Seq("count(*) AS result"))
        .shortestPath(
          "(a)-[r*1..]->(b)",
          pathName = Some("p"),
          nodePredicates = nodePredicates,
          relationshipPredicates = relPredicates
        )
        .cartesianProduct()
        .|.nodeByLabelScan("b", "B")
        .nodeByLabelScan("a", "A")
        .build()
    }
  }

  test("should not plan selection in shortestPath fallback plan if there are no predicates to solve") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(50)
      .build()

    val q =
      """
        |MATCH shortestPath((a)-[r*1..3]->(b))
        |MATCH (c)-[r*]->(d)
        |RETURN r
        |""".stripMargin

    val plan = planner.plan(q)

    // shortestPath fallback ends with Top
    val topPlan = plan.folder.treeFindByClass[Top].value
    topPlan.folder.treeFindByClass[Selection] shouldBe empty
  }

  private def outgoingPathExpression(fromNode: String, rels: String, toNode: String) = {
    PathExpression(
      NodePathStep(
        varFor(fromNode),
        multiRelationshipPathStep(rels, OUTGOING, toNode)
      )(pos)
    )(pos)
  }

  private def multiRelationshipPathStep(
    rel: String,
    dir: SemanticDirection,
    toNode: String
  ): MultiRelationshipPathStep =
    MultiRelationshipPathStep(varFor(rel), dir, Some(varFor(toNode)), NilPathStep()(pos))(pos)

}
