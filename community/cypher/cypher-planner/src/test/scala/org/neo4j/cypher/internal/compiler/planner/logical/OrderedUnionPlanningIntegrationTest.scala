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

import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedParallel
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class OrderedUnionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  private val planner = plannerBuilder()
    .setAllNodesCardinality(100)
    .setLabelCardinality("A", 60)
    .setLabelCardinality("B", 60)
    .build()

  test("should use UnionNodeByLabelsScan for Label disjunction") {
    val query = "MATCH (m) WHERE m:A OR m:B RETURN m"

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .unionNodeByLabelsScan("m", Seq("A", "B"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should use UnionNodeByLabelsScan Label disjunction between 3 labels") {
    val query = "MATCH (m) WHERE m:A OR m:B OR m:C RETURN m"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .setLabelCardinality("C", 60)
      .build()
    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .unionNodeByLabelsScan("m", Seq("A", "B", "C"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should not use ordered union for Label disjunction between 2 labels and a property predicatwe") {
    val query = "MATCH (m:C) WHERE m:A OR m:B OR m.prop > 0 RETURN m"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .setLabelCardinality("C", 60)
      .addNodeIndex("C", Seq("prop"), 1.0, 0.1)
      .build()
      .plan(query)
      .stripProduceResults

    withClue(plan) {
      plan.folder.treeCount {
        case _: OrderedDistinct => ()
      } should be(0)
    }
  }

  test("should use UnionNodeByLabelsScan for Label disjunction between 2 labels inside a conjunction") {
    val query = "MATCH (m)-[r]-(o) WHERE (m:A OR m:B) AND o.prop = 0 RETURN m"

    val plan = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .setRelationshipCardinality("(:A)-[]->()", 100)
      .setRelationshipCardinality("()-[]->(:A)", 100)
      .setRelationshipCardinality("(:B)-[]->()", 100)
      .setRelationshipCardinality("()-[]->(:B)", 100)
      .build()
      .plan(query)
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .filter("o.prop = 0")
        .expand("(m)-[r]-(o)")
        .unionNodeByLabelsScan("m", Seq("A", "B"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should use UnionNodeByLabelsScan for Label disjunction in a WITHs WHERE clause") {
    val query = "MATCH (m) WITH m WHERE m:A OR m:B RETURN m"

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .unionNodeByLabelsScan("m", Seq("A", "B"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should use UnionNodeByLabelsScan for Label disjunction in tail") {
    val query = "MATCH (n) WITH n LIMIT 1 MATCH (m) WHERE m:A OR m:B RETURN m"

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.unionNodeByLabelsScan("m", Seq("A", "B"), "n")
        .limit(1)
        .allNodeScan("n")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should use UnionNodeByLabelsScan for Label in OPTIONAL MATCH") {
    val query = "OPTIONAL MATCH (m) WHERE m:A OR m:B RETURN m"

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .optional()
        .unionNodeByLabelsScan("m", Seq("A", "B"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should use UnionNodeByLabelsScan for Label disjunction with DISTINCT") {
    val query = "MATCH (n) WHERE n:A OR n:B RETURN DISTINCT n"

    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .unionNodeByLabelsScan("n", Seq("A", "B"), IndexOrderAscending)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should not use ordered union in parallel runtime") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .setLabelCardinality("B", 500)
      .addNodeExistenceConstraint("A", "prop")
      .setExecutionModel(BatchedParallel(1, 2))
      .build()

    val plan = cfg.plan("MATCH (a:A|B) WHERE a.prop IS NOT NULL RETURN a")
    plan shouldEqual cfg.planBuilder()
      .produceResults("a")
      .distinct("a AS a")
      .union()
      .|.filter("a.prop IS NOT NULL")
      .|.nodeByLabelScan("a", "B")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("should use normal union for predicate disjunction with ORDER BY") {
    val query = "MATCH (p:Person) WHERE p.name < 0 OR p.name > 5 RETURN p.name ORDER BY p.name"

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 60)
      .addNodeIndex("Person", Seq("name"), 1.0, 0.01)
      .build()
    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .sort("`p.name` ASC")
        .projection("cacheN[p.name] AS `p.name`")
        .distinct("p AS p")
        .union()
        .|.nodeIndexOperator(
          "p:Person(name > 5)",
          _ => GetValue,
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .nodeIndexOperator(
          "p:Person(name < 0)",
          _ => GetValue,
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .build()
    )(SymmetricalLogicalPlanEquality)
  }
}
