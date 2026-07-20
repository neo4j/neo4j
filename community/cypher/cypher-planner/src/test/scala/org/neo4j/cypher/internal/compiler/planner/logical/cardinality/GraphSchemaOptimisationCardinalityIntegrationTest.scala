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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport

class GraphSchemaOptimisationCardinalityIntegrationTest extends CypherPlannerTestSuite
    with CardinalityIntegrationTestSupport
    with LogicalPlanningAttributesTestSupport {

  private val allNodes: Double = 1000.0
  private val allRels: Double = 50000.0
  private val bCount: Double = 100.0
  // private val bSel: Double = bCount / allNodes // = 0.1
  private val cCount: Double = 50.0
  private val cSel: Double = cCount / allNodes // = 0.05
  private val r1Count: Double = 3000.0
  private val r1BCount: Double = 1500.0
  private val r1CCount: Double = 800.0
  private val r2Count: Double = 5000.0
  private val bR2Count: Double = 3500.0
  private val r2BCount: Double = 2500.0
  private val bR2BCount: Double = 1800.0
  private val r2CCount: Double = 2000.0
  private val r3Count: Double = 3000.0
  private val r3BCount: Double = 1000.0
  private val r3CCount: Double = 2200.0

  private val planner =
    plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setAllRelationshipsCardinality(allRels)
      .setLabelCardinality("B", bCount)
      .setLabelCardinality("C", cCount)
      .setRelationshipCardinality("()-[:R1]->()", r1Count)
      .setRelationshipCardinality("()-[:R1]->(:B)", r1BCount)
      .setRelationshipCardinality("()-[:R1]->(:C)", r1CCount)
      .setRelationshipCardinality("()-[:R2]->()", r2Count)
      .setRelationshipCardinality("(:B)-[:R2]->()", bR2Count)
      .setRelationshipCardinality("()-[:R2]->(:B)", r2BCount)
      .setRelationshipCardinality("(:B)-[:R2]->(:B)", bR2BCount)
      .setRelationshipCardinality("()-[:R2]->(:C)", r2CCount)
      .setRelationshipCardinality("()-[:R3]->()", r3Count)
      .setRelationshipCardinality("()-[:R3]->(:B)", r3BCount)
      .setRelationshipCardinality("()-[:R3]->(:C)", r3CCount)

  // Cardinality estimation for queries with relationship endpoint constraints goes as follows:
  // 1. Add labels to the pattern that can be obtained from the constraint
  // 2. Simplify the cardinality estimation for single relationship hops if there is a constraint on this relationship

  test("MATCH (a)-[:R1]->(b)<-[:R2]-(c)") {
    queryShouldHaveCardinality(
      planner.build(),
      "MATCH (a)-[:R1]->(b)<-[:R2]-(c)",
      r1Count * r2Count / allNodes
    )
  }

  test("MATCH (a)-[:R1]->(b:B)<-[:R2]-(c)") {
    queryShouldHaveCardinality(
      planner.build(),
      "MATCH (a)-[:R1]->(b:B)<-[:R2]-(c)",
      r1BCount * r2BCount / bCount
    )
  }

  test("MATCH (b:B)<-[:R2]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (b:B)<-[:R2]-(c)",
      r2Count
    )
  }

  test("MATCH (b)<-[:R2]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (b:B)<-[:R2]-(c)",
      r2Count
    )
  }

  test("MATCH (b:B:C)<-[:R2]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (b:B:C)<-[:R2]-(c)",
      r2CCount
    )
  }

  test("MATCH (b:C)<-[:R2]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (b:C)<-[:R2]-(c)",
      r2CCount
    )
  }

  test("MATCH (a)-[:R1]->(b:B)<-[:R2]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:B)<-[:R2]-(c)",
      r1BCount * r2Count / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b)<-[:R2]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b)<-[:R2]-(c)",
      r1BCount * r2Count / bCount
    )
  }

  test("MATCH (a)-[:R1]->(a)") {
    queryShouldHaveCardinality(
      planner.build(),
      "MATCH (a)-[:R1]->(a)",
      r1Count / allNodes
    )
  }

  test("MATCH (a)-[:R1]-(a)") {
    queryShouldHaveCardinality(
      planner.build(),
      "MATCH (a)-[:R1]-(a)",
      (r1Count + r1Count) / allNodes
    )
  }

  test("MATCH (a:B)-[:R2]-(a)") {
    queryShouldHaveCardinality(
      planner.build(),
      "MATCH (a:B)-[:R2]-(a)",
      (bR2BCount + bR2BCount) / bCount
    )
  }

  test("MATCH (a:B)-[:R2]-(a) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a:B)-[:R2]-(a)",
      (r2Count + r2Count) / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b:B)-[:R2]-(b) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .setRelationshipCardinality("(:C)-[:R2]->()", r2Count)
        .setRelationshipCardinality("(:C)-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:B)-[:R2]-(b)",
      (r1BCount) * (r2Count + r2Count) / bCount / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b:B:C)-[:R2]-(b) constraint on C-R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .setRelationshipCardinality("(:C)-[:R2]->()", r2Count)
        .setRelationshipCardinality("(:C)-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("(:C)-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:B:C)-[:R2]-(b)",
      (r1BCount * cSel) * (r2Count + r2Count) / (bCount * cSel) / (bCount * cSel)
    )
  }

  test("MATCH (a)-[:R1]->(b)-[:R2]-(b) constraint on C-R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .setRelationshipCardinality("(:C)-[:R2]->()", r2Count)
        .setRelationshipCardinality("(:C)-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("(:C)-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b)-[:R2]-(b)",
      (r1BCount * cSel) * (r2Count + r2Count) / (bCount * cSel) / (bCount * cSel)
    )
  }

  // Disjunction
  test("MATCH (a)-[:R1]->(b)<-[:R2|R3]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b)<-[:R2|R3]-(c)",
      r1Count * (r2Count + r3Count) / allNodes
    )
  }

  test("MATCH (a)-[:R1]->(b:B)<-[:R2|R3]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:B)<-[:R2|R3]-(c)",
      r1BCount * (r2Count + r3BCount) / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b)<-[:R2|R3]-(c) constraint on R2->B, R3->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .setRelationshipCardinality("()-[:R3]->(:B)", r3Count)
        .addRelationshipEndpointLabelConstraint("()-[:R3]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b)<-[:R2|R3]-(c)",
      r1BCount * (r2Count + r3Count) / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b:B)<-[:R2|R3]-(c) constraint on R2->B, R3->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .setRelationshipCardinality("()-[:R3]->(:B)", r3Count)
        .addRelationshipEndpointLabelConstraint("()-[:R3]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:B)<-[:R2|R3]-(c)",
      r1BCount * (r2Count + r3Count) / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b:C)<-[:R2|R3]-(c) constraint on R2->B, R3->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .setRelationshipCardinality("()-[:R3]->(:B)", r3Count)
        .addRelationshipEndpointLabelConstraint("()-[:R3]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:C)<-[:R2|R3]-(c)",
      // r1BCount * cSel = 1500.0 * 0.05 = 75.0
      // r1CCount * bSel = 800.0 * 0.1 = 80.0 > 75.0
      (r1BCount * cSel) * (r2CCount + r3CCount) / (bCount * cSel)
    )
  }

  test("MATCH (a)-[:R1]->(b:B:C)<-[:R2|R3]-(c) constraint on R2->B, R3->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .setRelationshipCardinality("()-[:R3]->(:B)", r3Count)
        .addRelationshipEndpointLabelConstraint("()-[:R3]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:B:C)<-[:R2|R3]-(c)",
      // r1BCount * cSel = 1500.0 * 0.05 = 75.0
      // r1CCount * bSel = 800.0 * 0.1 = 80.0 > 75.0
      (r1BCount * cSel) * (r2CCount + r3CCount) / (bCount * cSel)
    )
  }

  test("MATCH (a)-[:R1]->(b:B)<-[:R2]-(c) constraint on R1->B, R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R1]->(:B)", r1Count)
        .addRelationshipEndpointLabelConstraint("()-[:R1]->(:B)")
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:B)<-[:R2]-(c)",
      r1Count * r2Count / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b)<-[:R2]-(c) constraint on R1->B, R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R1]->(:B)", r1Count)
        .addRelationshipEndpointLabelConstraint("()-[:R1]->(:B)")
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b)<-[:R2]-(c)",
      r1Count * r2Count / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b:B:C)<-[:R2]-(c) constraint on R1->B, R1->C") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R1]->(:B)", r1Count)
        .addRelationshipEndpointLabelConstraint("()-[:R1]->(:B)")
        .setRelationshipCardinality("()-[:R1]->(:C)", r1Count)
        .addRelationshipEndpointLabelConstraint("()-[:R1]->(:C)")
        .build(),
      "MATCH (a)-[:R1]->(b:B:C)<-[:R2]-(c)",
      r1Count * (cSel * r2BCount) / (bCount * cSel)
    )
  }

  test("MATCH (a)-[:R1]->(b:B:C)<-[:R2]-(c) constraint on R1->B, R2->C") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R1]->(:B)", r1Count)
        .addRelationshipEndpointLabelConstraint("()-[:R1]->(:B)")
        .setRelationshipCardinality("()-[:R2]->(:C)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:C)")
        .build(),
      "MATCH (a)-[:R1]->(b:B:C)<-[:R2]-(c)",
      r1CCount * r2BCount / (bCount * cSel)
    )
  }

  test("MATCH (a)-[:R1]->(b)<-[:R2]-(c) constraint on R1->B, R2->C") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R1]->(:B)", r1Count)
        .addRelationshipEndpointLabelConstraint("()-[:R1]->(:B)")
        .setRelationshipCardinality("()-[:R2]->(:C)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:C)")
        .build(),
      "MATCH (a)-[:R1]->(b)<-[:R2]-(c)",
      r1CCount * r2BCount / (bCount * cSel)
    )
  }

  test("MATCH (a)-[:R1]->(b)-[:R2]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b)-[:R2]-(c)",
      r1Count * (r2Count /* in */ + r2Count /* out */ ) / allNodes
    )
  }

  test("MATCH (a)-[:R1]->(b:B)-[:R2]-(c) constraint on R2->B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]->(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:B)-[:R2]-(c)",
      r1BCount * (r2Count /* in */ + bR2Count /* out */ ) / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b)-[:R2]-(c) constraint on R2-B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]-(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]-(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b)-[:R2]-(c)",
      r1BCount * (r2Count /* in */ + r2Count /* out */ ) / bCount
    )
  }

  test("MATCH (a)-[:R1]->(b:B)-[:R2]-(c) constraint on R2-B") {
    queryShouldHaveCardinality(
      planner
        .setRelationshipCardinality("()-[:R2]-(:B)", r2Count)
        .addRelationshipEndpointLabelConstraint("()-[:R2]-(:B)")
        .build(),
      "MATCH (a)-[:R1]->(b:B)-[:R2]-(c)",
      r1BCount * (r2Count /* in */ + r2Count /* out */ ) / bCount
    )
  }
}
