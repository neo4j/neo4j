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
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.SpdSelections.SpdSelectionAndChild

class SpdSelectionsUnapplyTest extends CypherPlannerTestSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  private val planner = plannerBuilder()
    .setAllNodesCardinality(1000)
    .setAllRelationshipsCardinality(1500)
    .setLabelCardinality("A", 800)
    .setLabelCardinality("B", 600)
    .setRelationshipCardinality("()-[:R]->()", 500)
    .setRelationshipCardinality("(:A)-[:R]->()", 400)
    .setRelationshipCardinality("()-[:R]->(:B)", 450)
    .setRelationshipCardinality("(:A)-[:R]->(:B)", 300)
    .build()

  test("should not match anything for a plan without remoteBatchProperties") {
    val plan = planner.subPlanBuilder()
      .filter("b.prop = 2", "b:B")
      .expandAll("(a)-[r:R]->(b)")
      .filter("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections.isEmpty shouldBe true
  }

  test(
    "should not match anything for a plan that does not start with a selection or remoteBatchProperties(WithFilter)"
  ) {
    val plan = planner.subPlanBuilder()
      .expandAll("(b)-[r2:R]->(c)")
      .filter("cacheN[b.prop] = 2")
      .remoteBatchProperties("cacheNFromStore[b.prop]")
      .filter("b:Person")
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections.isEmpty shouldBe true
  }

  test("should match case 1111") {
    val plan = planner.subPlanBuilder()
      .filter("cacheN[b.prop2] = 2")
      .remoteBatchProperties("cacheNFromStore[b.prop2]")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.prop]")("b.prop = 1")
      .filter("b:Person")
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set(hasLabels("b", "Person")), // b:Person
        filterOnShards = Some(PushedPredicatesDetails(
          SpdSelections.dummyVar,
          Set(equals(prop("b", "prop"), literalInt(1))),
          Set.empty,
          Set.empty
        )), // b.prop = 1
        filterOnMainWithRemoteProperties =
          Set(equals(prop("b", "prop2"), literalInt(2))) // b.prop2 = 2
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 1110") {
    val plan = planner.subPlanBuilder()
      .filter("cacheN[b.prop2] = 2")
      .remoteBatchProperties("cacheNFromStore[b.prop2]")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.prop]")("b.prop = 1")
      // None filter
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set.empty, // Nothing
        filterOnShards = Some(PushedPredicatesDetails(
          SpdSelections.dummyVar,
          Set(equals(prop("b", "prop"), literalInt(1))),
          Set.empty,
          Set.empty
        )), // b.prop = 1
        filterOnMainWithRemoteProperties =
          Set(equals(prop("b", "prop2"), literalInt(2))) // b.prop2 = 2
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 1101") {
    val plan = planner.subPlanBuilder()
      .filter("cacheN[b.prop2] = 2")
      .remoteBatchProperties("cacheNFromStore[b.prop2]")
      // None remoteBatchPropertiesWithFilter
      .filter("b:Person")
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set(hasLabels("b", "Person")), // b:Person
        filterOnShards = None, // Nothing
        filterOnMainWithRemoteProperties =
          Set(equals(prop("b", "prop2"), literalInt(2))) // b.prop2 = 2
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 1100") {
    val plan = planner.subPlanBuilder()
      .filter("cacheN[b.prop2] = 2")
      .remoteBatchProperties("cacheNFromStore[b.prop2]")
      // None remoteBatchPropertiesWithFilter
      // None filter
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set.empty, // Nothing
        filterOnShards = None, // Nothing
        filterOnMainWithRemoteProperties =
          Set(equals(prop("b", "prop2"), literalInt(2))) // b.prop2 = 2
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 1011") {
    val plan = planner.subPlanBuilder()
      .filter("cacheN[a.prop] = cacheN[b.prop]")
      // None remoteBatchProperties
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.prop]")("b.prop = 1")
      .filter("b:Person")
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set(hasLabels("b", "Person")), // b:Person
        filterOnShards = Some(PushedPredicatesDetails(
          SpdSelections.dummyVar,
          Set(equals(prop("b", "prop"), literalInt(1))),
          Set.empty,
          Set.empty
        )), // b.prop = 1
        filterOnMainWithRemoteProperties = Set(
          equals(prop("a", "prop"), prop("b", "prop"))
        ) // a.prop = b.prop
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 1010") {
    val plan = planner.subPlanBuilder()
      .filter("cacheN[a.prop] = cacheN[b.prop]")
      // None remoteBatchProperties
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.prop]")("b.prop = 1")
      // None filter
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set.empty, // Nothing
        filterOnShards = Some(PushedPredicatesDetails(
          SpdSelections.dummyVar,
          Set(equals(prop("b", "prop"), literalInt(1))),
          Set.empty,
          Set.empty
        )), // b.prop = 1
        filterOnMainWithRemoteProperties = Set(
          equals(prop("a", "prop"), prop("b", "prop"))
        ) // a.prop = b.prop
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 0111") {
    val plan = planner.subPlanBuilder()
      // None filter
      .remoteBatchProperties("cacheNFromStore[b.prop2]")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.prop]")("b.prop = 1")
      .filter("b:Person")
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set(hasLabels("b", "Person")), // b:Person
        filterOnShards = Some(PushedPredicatesDetails(
          SpdSelections.dummyVar,
          Set(equals(prop("b", "prop"), literalInt(1))),
          Set.empty,
          Set.empty
        )), // b.prop = 1
        filterOnMainWithRemoteProperties = Set.empty // Nothing
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 0110") {
    val plan = planner.subPlanBuilder()
      // None filter
      .remoteBatchProperties("cacheNFromStore[b.prop2]")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.prop]")("b.prop = 1")
      // None filter
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set.empty, // Nothing
        filterOnShards = Some(PushedPredicatesDetails(
          SpdSelections.dummyVar,
          Set(equals(prop("b", "prop"), literalInt(1))),
          Set.empty,
          Set.empty
        )), // b.prop = 1
        filterOnMainWithRemoteProperties = Set.empty // Nothing
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 0101") {
    val plan = planner.subPlanBuilder()
      // None
      .remoteBatchProperties("cacheNFromStore[b.prop2]")
      // None
      .filter("b:Person")
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set(hasLabels("b", "Person")), // b:Person
        filterOnShards = None, // Nothing
        filterOnMainWithRemoteProperties = Set.empty // Nothing
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 0100") {
    val plan = planner.subPlanBuilder()
      // None filter
      .remoteBatchProperties("cacheNFromStore[b.prop2]")
      // None remoteBatchPropertiesWithFilter
      // None filter
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set.empty, // Nothing
        filterOnShards = None, // Nothing
        filterOnMainWithRemoteProperties = Set.empty // Nothing
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 0011") {
    val plan = planner.subPlanBuilder()
      // None filter
      // None remoteBatchProperties
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.prop]")("b.prop = 1")
      .filter("b:Person")
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set(hasLabels("b", "Person")), // b:Person
        filterOnShards = Some(PushedPredicatesDetails(
          SpdSelections.dummyVar,
          Set(equals(prop("b", "prop"), literalInt(1))),
          Set.empty,
          Set.empty
        )), // b.prop = 1
        filterOnMainWithRemoteProperties = Set.empty // Nothing
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }

  test("should match case 0010") {
    val plan = planner.subPlanBuilder()
      // None filter
      // None remoteBatchProperties
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.prop]")("b.prop = 1")
      // None filter
      .expandAll("(a)-[r:R]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
      .nodeByLabelScan("a", "A")
      .build()

    val maybeSpdSelections = SpdSelections.unapply(plan)
    maybeSpdSelections shouldBe Some(SpdSelectionAndChild(
      selections = ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set.empty, // Nothing
        filterOnShards = Some(PushedPredicatesDetails(
          SpdSelections.dummyVar,
          Set(equals(prop("b", "prop"), literalInt(1))),
          Set.empty,
          Set.empty
        )), // b.prop = 1
        filterOnMainWithRemoteProperties = Set.empty // Nothing
      ),
      child = planner.subPlanBuilder()
        .expandAll("(a)-[r:R]->(b)")
        .remoteBatchPropertiesWithFilter("cacheNFromStore[a.prop]")("a.prop = 1")
        .nodeByLabelScan("a", "A")
        .build()
    ))
  }
}
