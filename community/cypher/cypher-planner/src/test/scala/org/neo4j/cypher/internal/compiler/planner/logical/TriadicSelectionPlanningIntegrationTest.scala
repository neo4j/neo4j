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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class TriadicSelectionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  private val planner = plannerBuilder()
    .setAllNodesCardinality(1000)
    .setAllRelationshipsCardinality(200)
    .setLabelCardinality("X", 20)
    .setLabelCardinality("Y", 200)
    .setLabelCardinality("Z", 200)
    .setRelationshipCardinality("(:X)-[]->()", 20)
    .setRelationshipCardinality("(:Y)-[]->()", 20)
    .setRelationshipCardinality("(:Z)-[]->()", 20)
    .setRelationshipCardinality("()-[:A]->()", 20)
    .setRelationshipCardinality("(:X)-[:A]->()", 20)
    .setRelationshipCardinality("()-[:A]->(:X)", 20)
    .setRelationshipCardinality("()-[:B]->()", 20)
    .setRelationshipCardinality("(:X)-[]->(:Y)", 20)
    .setRelationshipCardinality("(:Y)-[]->(:Y)", 20)
    .setRelationshipCardinality("(:Y)-[]->(:Z)", 20)
    .setRelationshipCardinality("(:X)-[]->(:Z)", 20)
    .setRelationshipCardinality("(:Z)-[]->(:Z)", 20)
    .setRelationshipCardinality("()-[]->(:Y)", 20)
    .setRelationshipCardinality("()-[]->(:Z)", 20)
    .build()

  // Negative Predicate Expression

  test("MATCH (a:X)-->(b)-->(c) WHERE NOT (a)-->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b)-[r2]->(c) WHERE NOT (a)-->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .filter("not r2 = r1")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2]->(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  test("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE NOT (a)-->(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE NOT (a)-->(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE NOT (a)<-[:A]-(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE NOT (a)<-[:A]-(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE NOT (a:X)-[:A]->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1:A]->(b)-[r2:A]->(c) WHERE NOT (a)-[:A]->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .filter("not r2 = r1")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2:A]->(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1:A]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  test("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE NOT (a:X)-[:A]->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1:A]->(b)-[r2:B]->(c) WHERE NOT (a)-[:A]->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2:B]->(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1:A]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  test("MATCH (a:X)-[:A]->(b)<-[:B]-(c) WHERE NOT (a:X)-[:A]->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1:A]->(b)<-[r2:B]-(c) WHERE NOT (a)-[:A]->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)<-[r2:B]-(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1:A]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  // Positive Predicate Expression

  test("MATCH (a:X)-->(b)-[:A]->(c) WHERE (a:X)-[:A]->(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b)-[r2:A]->(c) WHERE (a)-[:A]->(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-->(b)-->(c) WHERE (a)-->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b)-[r2]->(c) WHERE (a)-->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .filter("not r2 = r1")
      .triadicSelection(positivePredicate = true, "a", "b", "c")
      .|.expandAll("(b)-[r2]->(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  test("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE (a)-->(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[r1:A]->(b)-[r2:B]->(c) WHERE (a)-->(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE (a)<-[:A]-(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[r1:A]->(b)-[r2:A]->(c) WHERE (a)<-[:A]-(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE (a:X)-[:A]->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1:A]->(b)-[r2:A]->(c) WHERE (a)-[:A]->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .filter("not r2 = r1")
      .triadicSelection(positivePredicate = true, "a", "b", "c")
      .|.expandAll("(b)-[r2:A]->(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1:A]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  test("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE (a:X)-[:A]->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1:A]->(b)-[r2:B]->(c) WHERE (a)-[:A]->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .triadicSelection(positivePredicate = true, "a", "b", "c")
      .|.expandAll("(b)-[r2:B]->(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1:A]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  test("MATCH (a:X)-[:A]->(b)<-[:B]-(c) WHERE (a:X)-[:A]->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1:A]->(b)<-[r2:B]-(c) WHERE (a)-[:A]->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .triadicSelection(positivePredicate = true, "a", "b", "c")
      .|.expandAll("(b)<-[r2:B]-(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1:A]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  // Negative Predicate Expression and matching labels

  test("MATCH (a:X)-->(b:Y)-->(c:Y) WHERE NOT (a)-->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c:Y) WHERE NOT (a)-->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .filter("not r2 = r1", "c:Y")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2]->(c)")
      .|.argument("b", "r1")
      .filter("b:Y")
      .expandAll("(a)-[r1]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  test("MATCH (a:X)-->(b:Y)-->(c:Z) WHERE NOT (a)-->(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c:Z) WHERE NOT (a)-->(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-->(b:Y)-->(c) WHERE NOT (a)-->(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c) WHERE NOT (a)-->(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-->(b)-->(c:Z) WHERE NOT (a)-->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b)-[r2]->(c:Z) WHERE NOT (a)-->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .filter("c:Z", "not r2 = r1")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2]->(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  test("MATCH (a:X)-->(b:Y:Z)-->(c:Z) WHERE NOT (a)-->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b:Y:Z)-[r2]->(c:Z) WHERE NOT (a)-->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .filter("c:Z", "not r2 = r1")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2]->(c)")
      .|.argument("b", "r1")
      .filterExpression(andsReorderable("b:Y", "b:Z"))
      .expandAll("(a)-[r1]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  // Positive Predicate Expression and matching labels

  test("MATCH (a:X)-->(b:Y)-->(c:Y) WHERE (a)-->(c)") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c:Y) WHERE (a)-->(c) RETURN 1").stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("1 AS 1")
      .filter("not r2 = r1", "c:Y")
      .triadicSelection(positivePredicate = true, "a", "b", "c")
      .|.expandAll("(b)-[r2]->(c)")
      .|.argument("b", "r1")
      .filter("b:Y")
      .expandAll("(a)-[r1]->(b)")
      .nodeByLabelScan("a", "X")
      .build())
  }

  test("MATCH (a:X)-->(b:Y)-->(c:Z) WHERE (a)-->(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c:Z) WHERE (a)-->(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-->(b:Y)-->(c) WHERE (a)-->(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c) WHERE (a)-->(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-->(b)-->(c:Z) WHERE (a)-->(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b)-[r2]->(c:Z) WHERE (a)-->(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }

  test("MATCH (a:X)-->(b:Y:Z)-->(c:Z) WHERE (a)-->(c) passes through") {
    val plan = planner.plan("MATCH (a:X)-[r1]->(b:Y:Z)-[r2]->(c:Z) WHERE (a)-->(c) RETURN 1").stripProduceResults

    plan.folder.treeExists {
      case _: TriadicSelection => true
    } should be(false)
  }
}
