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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class countStorePlannerConstraintTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  // NODES

  test("should not plan to obtain the count from count store when counting node properties without a label") {
    val query = "MATCH (n) RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test("should plan to obtain the count from count store when counting node properties with a constraint") {
    val query = "MATCH (n:A) RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 30)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(n.prop)`")
      .nodeCountFromCountStore("count(n.prop)", Seq(Some("A")))
      .build())
  }

  test(
    "should not plan to obtain the count from count store when counting node properties with a constraint but with two nodes in the pattern"
  ) {
    val query = "MATCH (n:A), (m:B) RETURN count(m.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 30)
      .setLabelCardinality("B", 30)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test(
    "should plan to obtain the count from count store when counting node properties with a constraint but with two nodes in the pattern"
  ) {
    val query = "MATCH (n:A), (m:B) RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 30)
      .setLabelCardinality("B", 30)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(n.prop)`")
      .nodeCountFromCountStore("count(n.prop)", Seq(Some("A"), Some("B")))
      .build())
  }

  test(
    "should not plan to obtain the count from count store when counting node properties with a constraint but with a more complex pattern"
  ) {
    val query = "MATCH (n:A)-[]-(m:A) RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 30)
      .setRelationshipCardinality("(:A)-[]->()", 40)
      .setRelationshipCardinality("()-[]->(:A)", 40)
      .setRelationshipCardinality("(:A)-[]->(:A)", 20)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test(
    "should plan to obtain the count from count store when counting node properties with a constraint when there is a conflicting predicate in the query"
  ) {
    val query = "MATCH (n:A) WHERE n.prop < 0 RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 30)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test(
    "should not plan to obtain the count from count store when counting node properties with a constraint when there are several labels in the query"
  ) {
    val query = "MATCH (n:A:B) RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .addNodeExistenceConstraint("A", "prop")
      .addNodeExistenceConstraint("B", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test(
    "should not plan to obtain the count from count store when counting node properties without constraints for them"
  ) {
    val query = "MATCH (n:A) RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 10)
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  // RELATIONSHIP

  test(
    "should not plan to obtain the count from count store when counting relationship properties without a relationship type"
  ) {
    val query = "MATCH ()-[r]-() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: RelationshipCountFromCountStore =>
    })
  }

  test(
    "should not plan to obtain the count from count store when counting relationship properties with a constraint but without direction"
  ) {
    val query = "MATCH ()-[r:R]-() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .addRelationshipExistenceConstraint("R", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: RelationshipCountFromCountStore =>
    })
  }

  test(
    "should plan to obtain the count from count store when counting properties on a relationship constraint property with outgoing edge"
  ) {
    val query = "MATCH ()-[r:R]->() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .addRelationshipExistenceConstraint("R", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(r.prop)`")
      .relationshipCountFromCountStore("count(r.prop)", None, Seq("R"), None)
      .build())
  }

  test(
    "should plan to obtain the count from count store when counting properties on a relationship constraint property with incoming edge"
  ) {
    val query = "MATCH ()<-[r:R]-() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .addRelationshipExistenceConstraint("R", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(r.prop)`")
      .relationshipCountFromCountStore("count(r.prop)", None, Seq("R"), None)
      .build())
  }

  test(
    "should not plan to obtain the count from count store when counting relationship properties with a constraint but with a more complex pattern"
  ) {
    val query = "MATCH ()-[r:R]->()<-[p:R]-() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .addRelationshipExistenceConstraint("R", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: RelationshipCountFromCountStore =>
    })
  }

  test("should not plan to obtain the count from count store when counting relationship properties with a constraint but with a more complex pattern and " +
    "taking both values into account") {
    val query = "MATCH ()-[r:R]->()<-[p:R]-() RETURN count(r.prop) + count(p.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .addRelationshipExistenceConstraint("R", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: RelationshipCountFromCountStore =>
    })
  }

  test("should plan to obtain the count from count store when counting properties on a relationship constraint property with outgoing edge with a conflicting" +
    " predicate") {
    val query = "MATCH ()-[r:R]->() WHERE r.prop < 0 RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .addRelationshipExistenceConstraint("R", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: RelationshipCountFromCountStore =>
    })
  }

  test(
    "should plan to obtain the count from count store when counting properties with a constraint on several relationship types"
  ) {
    val query = "MATCH ()-[r:R|Q]->() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .setRelationshipCardinality("()-[:Q]-()", 10)
      .addRelationshipExistenceConstraint("R", "prop")
      .addRelationshipExistenceConstraint("Q", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(r.prop)`")
      .relationshipCountFromCountStore("count(r.prop)", None, Seq("R", "Q"), None)
      .build())
  }

  test("should not plan to obtain the count from count store when counting properties with a constraint on some of the several relationship types but not all" +
    " of them") {
    val query = "MATCH ()-[r:R|Q]->() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .setRelationshipCardinality("()-[:Q]-()", 10)
      .addRelationshipExistenceConstraint("R", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: RelationshipCountFromCountStore =>
    })
  }

  test(
    "should not plan to obtain the count from count store when counting properties with several relationship types but no constraints for them"
  ) {
    val query = "MATCH ()-[r:R|Q]->() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .setRelationshipCardinality("()-[:Q]-()", 10)
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: RelationshipCountFromCountStore =>
    })
  }

  test("should not plan to obtain the count from count store when counting properties with a constraint with several relationships" +
    "for them") {
    val query = "MATCH ()-[r:R]->(), ()-[q:Q]->() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .setRelationshipCardinality("()-[:Q]-()", 10)
      .addRelationshipExistenceConstraint("R", "prop")
      .addRelationshipExistenceConstraint("Q", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: RelationshipCountFromCountStore =>
    })
  }

  test(
    "should plan to obtain the count from count store when counting properties with a constraint with several relationship types and a label on one node"
  ) {
    val query = "MATCH (a:A)-[r:R|Q]->() RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 30)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .setRelationshipCardinality("()-[:Q]-()", 10)
      .setRelationshipCardinality("()-[:R]-(:A)", 7)
      .setRelationshipCardinality("()-[:Q]-(:A)", 7)
      .addRelationshipExistenceConstraint("R", "prop")
      .addRelationshipExistenceConstraint("Q", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(r.prop)`")
      .relationshipCountFromCountStore("count(r.prop)", Some("A"), Seq("R", "Q"), None)
      .build())
  }

  test(
    "should not plan to obtain the count from count store when counting properties with a constraint and labels on both node"
  ) {
    val query = "MATCH (a:A)-[r:R]->(b:B) RETURN count(r.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 30)
      .setLabelCardinality("B", 25)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:R]-()", 10)
      .setRelationshipCardinality("()-[:R]-(:A)", 7)
      .setRelationshipCardinality("()-[:R]-(:B)", 7)
      .setRelationshipCardinality("(:A)-[:R]-(:B)", 5)
      .addRelationshipExistenceConstraint("R", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: RelationshipCountFromCountStore =>
    })
  }

  test(
    "should plan to obtain the count from count store when counting node properties with a constraint when there is a non-conflicting predicate in the query"
  ) {
    val query = "MATCH (n:A) WHERE n.prop IS NOT NULL RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 30)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(n.prop)`")
      .nodeCountFromCountStore("count(n.prop)", Seq(Some("A")))
      .build())
  }

  test(
    "should not plan to obtain the count from count store when the IS NOT NULL predicate cannot be implied since the property key does not have an existence constraint"
  ) {
    val query = "MATCH (n:A) WHERE n.prop2 IS NOT NULL RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 30)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test(
    "should plan to obtain the count from count store by identifying that the IS NOT NULL predicate is implied by a label predicate and an existence constraint"
  ) {
    val query = "MATCH (n:A) WHERE n.prop2 IS NOT NULL RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 30)
      .addNodeExistenceConstraint("A", "prop")
      .addNodeExistenceConstraint("A", "prop2")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(n.prop)`")
      .nodeCountFromCountStore("count(n.prop)", Seq(Some("A")))
      .build())
  }

  test(
    "should not plan to obtain the count from count store when the IS NOT NULL predicate cannot be implied since the node does not have a label"
  ) {
    val query = "MATCH (n:A),(m) WHERE m.prop IS NOT NULL RETURN count(n.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 30)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test(
    "should plan to obtain the count from count store when counting relationship properties with a constraint when there is a non-conflicting predicate in the query"
  ) {
    val query = "MATCH ()-[e:T]->() WHERE e.prop IS NOT NULL RETURN count(e.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 30)
      .addRelationshipExistenceConstraint("T", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(e.prop)`")
      .relationshipCountFromCountStore("count(e.prop)", None, Seq("T"), None)
      .build())
  }

  test(
    "should not plan to obtain the relationship count from count store when the IS NOT NULL predicate cannot be implied since the variable does not have a label"
  ) {
    val query = "MATCH (n)-[e:T]->() WHERE n.prop IS NOT NULL RETURN count(e.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 30)
      .addRelationshipExistenceConstraint("T", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test(
    "should not plan to obtain the relationship count from count store when the IS NOT NULL predicate cannot be implied since the property key does not have an existence constraint"
  ) {
    val query = "MATCH ()-[e:T]->() WHERE e.prop2 IS NOT NULL RETURN count(e.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 30)
      .addRelationshipExistenceConstraint("T", "prop")
      .build()

    val plan = planner
      .plan(query)

    plan should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test(
    "should plan to obtain the relationship count from count store by identifying that the IS NOT NULL predicate is implied by a type predicate and an existence constraint"
  ) {
    val query = "MATCH ()-[e:T]->() WHERE e.prop2 IS NOT NULL RETURN count(e.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 30)
      .addRelationshipExistenceConstraint("T", "prop")
      .addRelationshipExistenceConstraint("T", "prop2")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(e.prop)`")
      .relationshipCountFromCountStore("count(e.prop)", None, Seq("T"), None)
      .build())
  }

  test(
    "should plan to obtain the relationship count from count store by identifying that the IS NOT NULL predicate is implied by a label predicate and an existence constraint"
  ) {
    val query = "MATCH (n:A)-[e:T]->() WHERE n.prop2 IS NOT NULL RETURN count(e.prop)"
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 30)
      .setRelationshipCardinality("(:A)-[:T]->()", 20)
      .addRelationshipExistenceConstraint("T", "prop")
      .addNodeExistenceConstraint("A", "prop2")
      .build()

    val plan = planner
      .plan(query)

    plan should equal(planner.planBuilder()
      .produceResults("`count(e.prop)`")
      .relationshipCountFromCountStore("count(e.prop)", Some("A"), Seq("T"), None)
      .build())
  }
}
