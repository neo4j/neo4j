/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.varFor
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.pos
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class FindShortestPathsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

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
    val plan = cfg.plan("MATCH (a), (b), p = shortestPath((a)-[r]->(b)) WITH p WHERE length(p) > 1 RETURN p").stripProduceResults
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

    val plan = cfg.plan("MATCH (a:X)<-[r1]-(b)-[r2]->(c:X), p = shortestPath((a)-[r]->(c)) RETURN p").stripProduceResults
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

    val pathExpr =
      PathExpression(
        NodePathStep(varFor("a"),
          MultiRelationshipPathStep(varFor("anon_0"), SemanticDirection.OUTGOING, Some(varFor("b")),
            NilPathStep()(pos))(pos))(pos)
      )(pos)

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("p")
        .apply()
        .|.antiConditionalApply("p")
        .|.|.top(Seq(Ascending("anon_1")), 1)
        .|.|.projection("length(p) AS anon_1")
        .|.|.filter("length(p) > pathVal", "all(r IN relationships(p) WHERE cacheRFromStore[r.prop] = relVal)", "all(n IN nodes(p) WHERE cacheNFromStore[n.prop] = nodeVal)")
        .|.|.projection(Map("p" -> pathExpr))
        .|.|.expand("(a)-[anon_0*1..]->(b)", expandMode = ExpandInto, projectedDir = OUTGOING)
        .|.|.argument("relVal", "nodeVal", "a", "pathVal", "b")
        .|.apply()
        .|.|.optional("nodeVal", "pathVal", "b", "relVal", "a")
        .|.|.shortestPath("(a)-[anon_0*1..]->(b)", pathName = Some("p"), predicates = Seq("all(r IN relationships(p) WHERE cacheRFromStore[r.prop] = relVal)", "all(n IN nodes(p) WHERE cacheNFromStore[n.prop] = nodeVal)", "length(p) > pathVal"), withFallback = true)
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

      val predicates = Seq(
        s"ALL(x IN $nodesF(p) WHERE x.prop > 123)",
        s"ALL(y IN $relationshipsF(p) WHERE not y.prop = 'hello')",
        s"NONE(z IN $nodesF(p) WHERE z.otherProp < 321)",
        s"NONE(t IN $relationshipsF(p) WHERE t.otherProp IS NULL)"
      )

      val predicatesStr = predicates.mkString(" AND ")

      val q =
        s"""MATCH (a:A), (b:B)
           |MATCH p = shortestPath( (a)-[r*]->(b) )
           |WHERE $predicatesStr
           |RETURN count(*) AS result""".stripMargin

      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .aggregation(Seq(), Seq("count(*) AS result"))
        .shortestPath("(a)-[r*1..]->(b)", pathName = Some("p"), predicates = predicates)
        .cartesianProduct()
        .|.nodeByLabelScan("b", "B")
        .nodeByLabelScan("a", "A")
        .build()
    }
  }

}
