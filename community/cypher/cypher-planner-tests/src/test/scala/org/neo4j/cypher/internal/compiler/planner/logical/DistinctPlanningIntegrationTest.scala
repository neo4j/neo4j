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
import org.neo4j.cypher.internal.compiler.planner.UsingMatcher.using
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.GetValue

class DistinctPlanningIntegrationTest extends CypherPlannerTestSuite with LogicalPlanningIntegrationTestSupport {

  test("should not use eager plans for distinct") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) RETURN DISTINCT n.name")
    plan should not be using[Eager]
  }

  test("should not plan distinct after node unique index seek") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 50.0, isUnique = true)
      .build()

    val query = "MATCH (a:A) WHERE a.prop = 123 RETURN DISTINCT a AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("a AS result")
      .nodeIndexOperator("a:A(prop = 123)", getValue = _ => GetValue, unique = true)
      .build()
  }

  test("should not plan distinct after node unique composite index seek") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .addNodeIndex("A", Seq("x", "y"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 50.0, isUnique = true)
      .build()

    val query = "MATCH (a:A {x: 1, y: 2}) RETURN DISTINCT a AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("a AS result")
      .nodeIndexOperator("a:A(x = 1, y = 2)", getValue = _ => GetValue, unique = true, supportPartitionedScan = false)
      .build()
  }

  test("should plan distinct after node unique index range seek") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 50.0, isUnique = true)
      .build()

    val query = "MATCH (a:A) WHERE a.prop > 10 RETURN DISTINCT a AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .distinct("a AS result")
      .nodeIndexOperator("a:A(prop > 10)", getValue = _ => GetValue, unique = true)
      .build()
  }

  test("should not plan distinct after relationship unique index seek") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 50)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0 / 50.0,
        isUnique = true
      )
      .build()

    val query = "MATCH ()-[r:REL]->() WHERE r.prop = 123 RETURN DISTINCT r AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("r AS result")
      .relationshipIndexOperator("()-[r:REL(prop = 123)]->()", getValue = _ => GetValue, unique = true)
      .build()
  }

  test("should not plan distinct after relationship unique composite index seek") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 50)
      .addRelationshipIndex(
        "REL",
        Seq("x", "y"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0 / 50.0,
        isUnique = true
      )
      .build()

    val query = "MATCH ()-[r:REL {x: 1, y: 2}]->() RETURN DISTINCT r AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("r AS result")
      .relationshipIndexOperator("()-[r:REL(x = 1, y = 2)]->()", getValue = _ => GetValue, unique = true)
      .build()
  }

  test("should plan distinct after relationship unique index range seek") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 50)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0 / 50.0,
        isUnique = true
      )
      .build()

    val query = "MATCH ()-[r:REL]->() WHERE r.prop > 10 RETURN DISTINCT r AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .distinct("r AS result")
      .relationshipIndexOperator("()-[r:REL(prop > 10)]->()", getValue = _ => GetValue, unique = true)
      .build()
  }
}
