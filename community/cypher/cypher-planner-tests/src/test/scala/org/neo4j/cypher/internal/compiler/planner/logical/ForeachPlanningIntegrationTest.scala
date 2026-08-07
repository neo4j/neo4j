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

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

class ForeachPlanningIntegrationTest extends CypherPlannerTestSuite with LogicalPlanningIntegrationTestSupport {

  test("should be able to unnest apply for MERGE inside FOREACH") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("PROFILES", 100)
      .setRelationshipCardinality("()-[:RELATION]->()", 100)
      .setRelationshipCardinality("(:PROFILES)-[:RELATION]->()", 100)
      .setRelationshipCardinality("()-[:KNOWS]->()", 100)
      .setRelationshipCardinality("()-[:KNOWS]->(:PROFILES)", 100)
      .setRelationshipCardinality("()-[:RELATION]->(:PROFILES)", 0)
      .setRelationshipCardinality("(:PROFILES)-[:KNOWS]->()", 0)
      .build()

    val query =
      """MATCH (a:PROFILES { _key: $_key })-[r:RELATION]->(b)
        |WITH a, COLLECT(b) AS others
        |SET a.knows = size(others)
        |FOREACH (o IN others | 
        |  MERGE (o)-[:KNOWS]->(a)
        |)      
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .foreachApply("o", "others")
      .|.mergeInto("(o)-[anon_0:KNOWS]->(a)")
      .|.argument("a", "o")
      .setNodeProperty("a", "knows", "size(others)")
      .orderedAggregation(Seq("a AS a"), Seq("COLLECT(b) AS others"), Seq("a"))
      .expandAll("(a)-[:RELATION]->(b)")
      .filter("a._key = $_key")
      .nodeByLabelScan("a", "PROFILES", IndexOrderAscending)
      .build()
  }

  test("should be able to unnest apply for MERGE and SET inside FOREACH") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("PROFILES", 100)
      .setRelationshipCardinality("()-[:RELATION]->()", 100)
      .setRelationshipCardinality("(:PROFILES)-[:RELATION]->()", 100)
      .setRelationshipCardinality("()-[:KNOWS]->()", 100)
      .setRelationshipCardinality("()-[:KNOWS]->(:PROFILES)", 100)
      .setRelationshipCardinality("()-[:RELATION]->(:PROFILES)", 0)
      .setRelationshipCardinality("(:PROFILES)-[:KNOWS]->()", 0)
      .build()

    val query =
      """MATCH (a:PROFILES { _key: $_key })-[r:RELATION]->(b)
        |WITH a, COLLECT(b) AS others
        |SET a.knows = size(others)
        |FOREACH (o IN others |
        |  MERGE (o)-[:KNOWS]->(a)
        |  SET a.prop = others
        |)
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .foreachApply("o", "others")
      .|.setNodeProperty("a", "prop", "others")
      .|.mergeInto("(o)-[anon_0:KNOWS]->(a)")
      .|.argument("a", "o", "others")
      .setNodeProperty("a", "knows", "size(others)")
      .orderedAggregation(Seq("a AS a"), Seq("COLLECT(b) AS others"), Seq("a"))
      .expandAll("(a)-[:RELATION]->(b)")
      .filter("a._key = $_key")
      .nodeByLabelScan("a", "PROFILES", IndexOrderAscending)
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
}
