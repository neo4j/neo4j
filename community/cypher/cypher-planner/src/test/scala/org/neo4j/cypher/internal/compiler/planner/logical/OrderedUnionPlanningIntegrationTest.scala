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

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.MapKeys
import org.neo4j.graphdb.schema.IndexType

class OrderedUnionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  test("should use ordered union for Label disjunction") {
    val query = "MATCH (m) WHERE m:A OR m:B RETURN m"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .build()
      .plan(query)
      .stripProduceResults

    plan should (equal(new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("m"), "m AS m")
      .orderedUnion(Seq(Ascending("m")))
      .|.nodeByLabelScan("m", "A", IndexOrderAscending)
      .nodeByLabelScan("m", "B", IndexOrderAscending)
      .build())
      or equal(new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("m"), "m AS m")
      .orderedUnion(Seq(Ascending("m")))
      .|.nodeByLabelScan("m", "B", IndexOrderAscending)
      .nodeByLabelScan("m", "A", IndexOrderAscending)
      .build()))
  }

  test("should use ordered union for Label disjunction between 3 labels") {
    val query = "MATCH (m) WHERE m:A OR m:B OR m:C RETURN m"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .setLabelCardinality("C", 60)
      .build()
      .plan(query)
      .stripProduceResults

    plan should beLike {
      case OrderedDistinct(
      _:OrderedUnion,
      MapKeys("m"),
      Seq(Variable("m"))
      ) => ()
    }
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
      plan.treeCount {
        case _: OrderedDistinct => true
      } should be(0)
    }
  }

  test("should use ordered union for Label disjunction between 2 labels inside a conjunction") {
    val query = "MATCH (m)-[r]-(o) WHERE (m:A OR m:B) AND o.prop = 0 RETURN m"

    val plan = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .build()
      .plan(query)
      .stripProduceResults

    plan should (equal(new LogicalPlanBuilder(wholePlan = false)
      .filter("o.prop = 0")
      .expand("(m)-[r]-(o)")
      .orderedDistinct(Seq("m"), "m AS m")
      .orderedUnion(Seq(Ascending("m")))
      .|.nodeByLabelScan("m", "A", IndexOrderAscending)
      .nodeByLabelScan("m", "B", IndexOrderAscending)
      .build())
      or equal(new LogicalPlanBuilder(wholePlan = false)
      .filter("o.prop = 0")
      .expand("(m)-[r]-(o)")
      .orderedDistinct(Seq("m"), "m AS m")
      .orderedUnion(Seq(Ascending("m")))
      .|.nodeByLabelScan("m", "B", IndexOrderAscending)
      .nodeByLabelScan("m", "A", IndexOrderAscending)
      .build()))
  }

  test("should use ordered union for Label disjunction in a WITHs WHERE clause") {
    val query = "MATCH (m) WITH m WHERE m:A OR m:B RETURN m"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .build()
      .plan(query)
      .stripProduceResults

    plan should (equal(new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("m"), "m AS m")
      .orderedUnion(Seq(Ascending("m")))
      .|.nodeByLabelScan("m", "A", IndexOrderAscending)
      .nodeByLabelScan("m", "B", IndexOrderAscending)
      .build())
      or equal(new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("m"), "m AS m")
      .orderedUnion(Seq(Ascending("m")))
      .|.nodeByLabelScan("m", "B", IndexOrderAscending)
      .nodeByLabelScan("m", "A", IndexOrderAscending)
      .build()))
  }

  test("should use ordered union for Label disjunction in tail") {
    val query = "MATCH (n) WITH n LIMIT 1 MATCH (m) WHERE m:A OR m:B RETURN m"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .build()
      .plan(query)
      .stripProduceResults

    plan should (equal(new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.orderedDistinct(Seq("m"), "n AS n", "m AS m")
      .|.orderedUnion(Seq(Ascending("m")))
      .|.|.nodeByLabelScan("m", "A", IndexOrderAscending, "n")
      .|.nodeByLabelScan("m", "B", IndexOrderAscending, "n")
      .limit(1)
      .allNodeScan("n")
      .build())
      or equal(new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.orderedDistinct(Seq("m"), "n AS n", "m AS m")
      .|.orderedUnion(Seq(Ascending("m")))
      .|.|.nodeByLabelScan("m", "B", IndexOrderAscending, "n")
      .|.nodeByLabelScan("m", "A", IndexOrderAscending, "n")
      .limit(1)
      .allNodeScan("n")
      .build()))
  }

  test("should use ordered union for Label in OPTIONAL MATCH") {
    val query = "OPTIONAL MATCH (m) WHERE m:A OR m:B RETURN m"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .build()
      .plan(query)
      .stripProduceResults

    plan should (equal(new LogicalPlanBuilder(wholePlan = false)
      .optional()
      .orderedDistinct(Seq("m"), "m AS m")
      .orderedUnion(Seq(Ascending("m")))
      .|.nodeByLabelScan("m", "A", IndexOrderAscending)
      .nodeByLabelScan("m", "B", IndexOrderAscending)
      .build())
      or equal(new LogicalPlanBuilder(wholePlan = false)
      .optional()
      .orderedDistinct(Seq("m"), "m AS m")
      .orderedUnion(Seq(Ascending("m")))
      .|.nodeByLabelScan("m", "B", IndexOrderAscending)
      .nodeByLabelScan("m", "A", IndexOrderAscending)
      .build()))
  }

  test("should use ordered union for Label disjunction with DISTINCT") {
    val query = "MATCH (n) WHERE n:A OR n:B RETURN DISTINCT n"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 60)
      .setLabelCardinality("B", 60)
      .build()
      .plan(query)
      .stripProduceResults

    plan should (equal(new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("n"), "n AS n")
      .orderedDistinct(Seq("n"), "n AS n")
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "A", IndexOrderAscending)
      .nodeByLabelScan("n", "B", IndexOrderAscending)
      .build())
      or equal(new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("n"), "n AS n")
      .orderedDistinct(Seq("n"), "n AS n")
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "B", IndexOrderAscending)
      .nodeByLabelScan("n", "A", IndexOrderAscending)
      .build()))
  }

  test("should use normal union for predicate disjunction with ORDER BY") {
    val query = "MATCH (p:Person) WHERE p.name < 0 OR p.name > 5 RETURN p.name ORDER BY p.name"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 60)
      .addNodeIndex("Person", Seq("name"), 1.0, 0.01, providesOrder = IndexOrderCapability.BOTH)
      .build()
      .plan(query)
      .stripProduceResults

    plan should (equal(new LogicalPlanBuilder(wholePlan = false)
      .sort(Seq(Ascending("p.name")))
      .projection("p.name AS `p.name`")
      .distinct("p AS p")
      .union()
      .|.nodeIndexOperator("p:Person(name > 5)", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
      .nodeIndexOperator("p:Person(name < 0)", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
      .build())
      or equal(new LogicalPlanBuilder(wholePlan = false)
      .sort(Seq(Ascending("p.name")))
      .projection("p.name AS `p.name`")
      .distinct("p AS p")
      .union()
      .|.nodeIndexOperator("p:Person(name < 0)", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
      .nodeIndexOperator("p:Person(name > 5)", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
      .build()))
  }
}
