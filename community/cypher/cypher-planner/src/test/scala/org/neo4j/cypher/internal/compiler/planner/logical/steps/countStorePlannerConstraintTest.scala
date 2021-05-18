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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class countStorePlannerConstraintTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport with LogicalPlanningTestSupport2 {

  // NODES

  test("should not plan to obtain the count from count store when counting node properties without a label") {
    val plan = new given {
    } getLogicalPlanFor "MATCH (n) RETURN count(n.prop)"

    plan._2 should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test("should plan to obtain the count from count store when counting node properties with a constraint") {
    val plan = new given {
      existenceOrNodeKeyConstraintOn("A", Set("prop"))
    } getLogicalPlanFor "MATCH (n:A) RETURN count(n.prop)"

    plan._2 should containPlanMatching {
      case _: NodeCountFromCountStore =>
    }
  }

  test("should not plan to obtain the count from count store when counting node properties with a constraint but with two nodes in the pattern") {
    val plan = new given {
      existenceOrNodeKeyConstraintOn("A", Set("prop"))
    } getLogicalPlanFor "MATCH (n:A), (m:B) RETURN count(m.prop)"

    plan._2 should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test("should plan to obtain the count from count store when counting node properties with a constraint but with two nodes in the pattern") {
    val plan = new given {
      existenceOrNodeKeyConstraintOn("A", Set("prop"))
    } getLogicalPlanFor "MATCH (n:A), (m:B) RETURN count(n.prop)"

    plan._2 should containPlanMatching {
      case _: NodeCountFromCountStore =>
    }
  }

  test("should not plan to obtain the count from count store when counting node properties with a constraint but with a more complex pattern") {
    val plan = new given {
      existenceOrNodeKeyConstraintOn("A", Set("prop"))
    } getLogicalPlanFor "MATCH (n:A)-[]-(m:A) RETURN count(n.prop)"

    plan._2 should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test("should plan to obtain the count from count store when counting node properties with a constraint when there is a conflicting predicate in the query") {
    val plan = new given {
      existenceOrNodeKeyConstraintOn("A", Set("prop"))
    } getLogicalPlanFor "MATCH (n:A) WHERE n.prop < 0 RETURN count(n.prop)"

    plan._2 should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test("should not plan to obtain the count from count store when counting node properties with a constraint when there are several labels in the query") {
    val plan = new given {
      existenceOrNodeKeyConstraintOn("A", Set("prop"))
      existenceOrNodeKeyConstraintOn("B", Set("prop"))
    } getLogicalPlanFor "MATCH (n:A:B) RETURN count(n.prop)"

    plan._2 should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  test("should not plan to obtain the count from count store when counting node properties without constraints for them") {
    val plan = new given {
    } getLogicalPlanFor "MATCH (n:A) RETURN count(n.prop)"

    plan._2 should not(containPlanMatching {
      case _: NodeCountFromCountStore =>
    })
  }

  // RELATIONSHIP

  test("should not plan to obtain the count from count store when counting relationship properties without a relationship type") {
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
}
