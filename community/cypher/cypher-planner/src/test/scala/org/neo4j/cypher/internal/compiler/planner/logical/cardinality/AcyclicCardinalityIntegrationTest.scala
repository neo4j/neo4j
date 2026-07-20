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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_NODES_UNIQUENESS_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.RepetitionCardinalityModel

/**
 * Whenever there is more than one QPP in the path pattern, there may be too much or too few uniqueness checks.
 * The estimation of DisjointNodes is not accurate.
 */
class AcyclicCardinalityIntegrationTest extends CypherPlannerTestSuite with CardinalityIntegrationTestSupport {
  private val allNodes: Double = 1000.0
  private val allRels: Double = 50000.0
  private val rRels: Double = 100.0

  private val cardinalityChain1Rel2Nodes = allRels * DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor

  private val cardinalityChain2Rels3Nodes =
    allRels * (allRels / allNodes) * Math.pow(DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor, 3) // 2+1 = 3

  private val cardinalityChain3Rels4Nodes =
    allRels * (allRels / allNodes) * (allRels / allNodes) * Math.pow(
      DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor,
      6
    ) // 3+2+1 = 6

  private val cardinalityChain4Rels5Nodes =
    allRels * (allRels / allNodes) * (allRels / allNodes) * (allRels / allNodes) * Math.pow(
      DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor,
      10
    ) // 4+3+2+1 = 10

  private val cardinalityChain5Rels6Nodes =
    allRels * (allRels / allNodes) * (allRels / allNodes) * (allRels / allNodes) * (allRels / allNodes) * Math.pow(
      DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor,
      15
    ) // 5+4+3+2+1 = 15

  private def cardinalityChainWithNRels(n: Int): Double = {
    val nbNodes = n + 1
    allRels * Math.pow(allRels / allNodes, n - 1) *
      RepetitionCardinalityModel.nodeUniquenessSelectivity(nbNodes, 1).factor
  }

  private def cardinalityChainWithNRelsWithLabelR(n: Int): Double = {
    val nbNodes = n + 1
    rRels * Math.pow(rRels / allNodes, n - 1) * RepetitionCardinalityModel.nodeUniquenessSelectivity(nbNodes, 1).factor
  }

  private val planner =
    plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setAllRelationshipsCardinality(allRels)
      .setRelationshipCardinality("()-[:R]->()", rRels)

  test("Single relationship") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[r1]->(n2)",
      cardinalityChain1Rel2Nodes
    )
  }

  test("quantified relationship {1,1}") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[r1]->{1,1}(n2)",
      cardinalityChain1Rel2Nodes
    )
  }

  test("quantified relationship {2,4}") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[r1]->{2,4}(n2)",
      cardinalityChain2Rels3Nodes + cardinalityChain3Rels4Nodes + cardinalityChain4Rels5Nodes
    )
  }

  test("quantified relationship {,4}") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[r1]->{,4}(n2)",
      allNodes + cardinalityChain1Rel2Nodes + cardinalityChain2Rels3Nodes + cardinalityChain3Rels4Nodes + cardinalityChain4Rels5Nodes
    )
  }

  test("Two relationships in chain") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[r1]->(n2)-[r2]->(n3)",
      cardinalityChain2Rels3Nodes
    )
  }

  test("quantified relationship {2,2}") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1n2)-[r1]->{2,2}(n2n3)",
      cardinalityChain2Rels3Nodes
    )
  }

  test("QPP with 2 relationships {1,1}") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (start)((n1)-[r1]->(n2)-[r2]->(n3)){1,1}(end)",
      cardinalityChain2Rels3Nodes
    )
  }

  test("Chain with 4 relationships") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[r1]->(n2)-[r2]->(n3)-[r3]->(n4)-[r4]->(n5)",
      cardinalityChain4Rels5Nodes
    )
  }

  test("QPP with 2 relationships {2,2}") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)((n1n3)-[r1]->(n2n4)-[r2]->(n3n5)){2,2}(n5)",
      cardinalityChain4Rels5Nodes
    )
  }

  test("QPP with 1 relationships {4,4}") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)((n1234)-[]->(n2345)){4,4}(n5)",
      cardinalityChain4Rels5Nodes
    )
  }

  test("Chain of 2 relationships and QPP with 1 relationships {2,2}") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->(n2)-[]->(n3)((n3n4)-[]->(n4n5)){2,2}(n5)",
      cardinalityChain4Rels5Nodes
    )
  }

  test("Chain of 1 relationship and QPP with 1 relationships {1,1} and 1 relationship") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->(n2)(()-[]->()){1,1}(x)-[]->(y)",
      cardinalityChain3Rels4Nodes
    )
  }

  test("Chain of 1 relationship and QPP with 2 relationships {1,1} and 1 relationship") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->(n2)(()-[]->()-[]->()){1,1}(x)-[]->(y)",
      cardinalityChain4Rels5Nodes
    )
  }

  test("Chain of 1 relationship and QPP with 3 relationships {1,1} and 1 relationship") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->(n2)(()-[]->()-[]->()-[]->()){1,1}(x)-[]->(y)",
      cardinalityChain5Rels6Nodes
    )
  }

  test(
    "Chain of 1 relationship and QPP with 1 relationships {1,1} and QPP with 2 relationships {1,1} and 1 relationship"
  ) {
    // We assume for DisjointNodes-predicates that both QPPs belong to different equivalence classes.
    // When that is not the case, there are no NoneOfNodes predicates between the boundary nodes of the first QPP and
    // the nodes within the second QPP.
    // In that case we will miss some node uniqueness comparisons.
    // In this case we are missing the comparisons: NOT n2 = s, NOT n2 = t
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->(n2)(()-[]->()){1,1}(x)(()-[]->(s)-[]->(t)){1,1}(y)-[]->(z)",
      cardinalityChain5Rels6Nodes
        / DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor // Estimation error: missing NOT n2 = s
        / DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor // Estimation error: missing NOT n2 = t
    )
  }

  test("Chain of QPP with 2 relationships {1,1} and 1 relationship and QPP with 2 relationships {1,1}") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)((i1)-[]->(i2)-[]->(i3)){1,1}(x)-[]->(y)((i4)-[]->(i5)-[]->(i6)){1,1}(z)",
      cardinalityChain5Rels6Nodes
    )
  }

  test("Chain of 1 relationship and QPP with 1 relationships {2,2} and 1 relationship") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->(n2)((n2n3)-[]->(n3n4)){2,2}(n4)-[]->(n5)",
      cardinalityChain4Rels5Nodes
    )
  }

  test("Chain of 1 relationship and QPP with 1 relationships {1,2} and 1 relationship") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->(n2)(()-[]->()){1,2}(x)-[]->(y)",
      cardinalityChain3Rels4Nodes
        + cardinalityChain4Rels5Nodes
    )
  }

  test("Chain of 1 relationship and QPP with 1 relationships {,2} and 1 relationship") {
    // This query produces two uniqueness checks too much in the case of 0-iterations.
    // With 0-iterations, nodes n2 and x are the same.
    // Which means that DifferentNodes(x, n1) and DifferentNodes(x, y) are already implied by
    // DifferentNodes(n2, n1) and DifferentNodes(n2, y)
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->(n2)(()-[]->()){,2}(x)-[]->(y)",
      cardinalityChain2Rels3Nodes
        * DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor // Estimation error
        * DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor // Estimation error
        + cardinalityChain3Rels4Nodes
        + cardinalityChain4Rels5Nodes
    )
  }

  test("Chain of 1 relationship and QPP with 1 relationships {3,3} and 1 relationship") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->(n2)(()-[]->()){3,3}(x)-[]->(y)",
      cardinalityChain5Rels6Nodes
    )
  }

  test("Chain of QPP with 1 relationship {22, 22} and 3 relationships") {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->{22,22}(n2)-[]->(n3)-[]->(n4)-[]->(n5)",
      cardinalityChainWithNRels(25)
    )
  }

  test("Chain of QPP with 1 relationship {22, 22} and 1 relationships and QPP with 1 relationship {2,2}") {
    // The one inner non-boundary nodes in the second QPP is compared against 4 nodes (two from the inner pattern and two outer-boundary) of the first QPP instead of 23
    // This gives 23-4 = 19 missing node comparisons
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->{22,22}(n2)-[]->(n3)-[]->{2,2}(n5)",
      // Estimation error due assuming one iteration QPP1 in DisjointNodes(QPP1, QPP2)
      cardinalityChainWithNRels(25)
    )
  }

  test("Chain of QPP with 1 relationship {20, 22} and 1 relationships and QPP with 1 relationship {2,2}") {
    // The one inner non-boundary nodes in the second QPP is compared against 4 nodes (two from the inner pattern and two outer-boundary) of the first QPP instead of 23
    // This gives 23-4 = 19 missing node comparisons
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      "MATCH ACYCLIC (n1)-[]->{20,22}(n2)-[]->(n3)-[]->{2,2}(n5)",
      // Estimation error due assuming one iteration QPP1 in DisjointNodes(QPP1, QPP2)
      cardinalityChainWithNRels(23)
        * DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor // One node uniqueness check missing: we assumed 21 iterations for the first QPP, where only 20 occurred
        + cardinalityChainWithNRels(24)
        + cardinalityChainWithNRels(25)
        / DEFAULT_NODES_UNIQUENESS_SELECTIVITY.factor // One node uniqueness check too much: we assumed 21 iterations for the first QPP, where 22 occurred
    )
  }

  // The aim of the tests with SHORTEST is to make sure that WHEN the pattern cardinality is used, THEN acyclic semantics are taken into account
  test("ANY SHORTEST ACYCLIC (n1)-[]->{2,3}(n2)") {
    queryShouldHaveCardinality(
      planner.build(),
      CypherVersion.Cypher25,
      "MATCH ANY SHORTEST ACYCLIC (n1)-[]->{2,3}(n2)",
      // The cardinality estimate of the pattern 'ACYCLIC (n1)-[:R]->{2,3}(n2)' is not used, because it is larger than 'allNodes * allNodes'
      allNodes * allNodes
    )
  }

  test("ANY SHORTEST ACYCLIC (n1)-[:R]->{2,3}(n2)") {
    queryShouldHaveCardinality(
      planner.build(),
      CypherVersion.Cypher25,
      "MATCH ANY SHORTEST ACYCLIC (n1)-[:R]->{2,3}(n2)",
      // The cardinality estimate of the pattern 'ACYCLIC (n1)-[:R]->{2,3}(n2)' is used, because it is lower than 'allNodes * allNodes'
      cardinalityChainWithNRelsWithLabelR(2) + cardinalityChainWithNRelsWithLabelR(3)
    )
  }

  test("MATCH SHORTEST 5 ACYCLIC (n1)-[]->{2,3}(n2)") {
    queryShouldHaveCardinality(
      planner.build(),
      CypherVersion.Cypher25,
      "MATCH SHORTEST 5 ACYCLIC (n1)-[]->{2,3}(n2)",
      // The cardinality estimate of the pattern 'ACYCLIC (n1)-[]->{2,3}(n2)' is not used, because it is larger than 'allNodes * allNodes * 5'
      allNodes * allNodes * 5
    )
  }

  test("MATCH SHORTEST 500 ACYCLIC (n1)-[]->{2,3}(n2)") {
    queryShouldHaveCardinality(
      planner.build(),
      CypherVersion.Cypher25,
      "MATCH SHORTEST 500 ACYCLIC (n1)-[]->{2,3}(n2)",
      // The cardinality estimate of the pattern 'ACYCLIC (n1)-[]->{2,3}(n2)' is used, because it is smaller than 'allNodes * allNodes * 500'
      cardinalityChain2Rels3Nodes + cardinalityChain3Rels4Nodes
    )
  }

  test("MATCH SHORTEST 5 ACYCLIC (n1)-[:R]->{2,3}(n2)") {
    queryShouldHaveCardinality(
      planner.build(),
      CypherVersion.Cypher25,
      "MATCH SHORTEST 5 ACYCLIC (n1)-[:R]->{2,3}(n2)",
      // The cardinality estimate of the pattern 'ACYCLIC (n1)-[:R]->{2,3}(n2)' is used, because it is lower than 'allNodes * allNodes * 5'
      cardinalityChainWithNRelsWithLabelR(2) + cardinalityChainWithNRelsWithLabelR(3)
    )
  }

  test("SHORTEST 1 ACYCLIC GROUP (n1)-[]->{2,5}(n2)") {
    queryShouldHaveCardinality(
      planner.build(),
      CypherVersion.Cypher25,
      "MATCH SHORTEST 1 ACYCLIC GROUP (n1)-[]->{2,5}(n2)",
      // The cardinality estimate of the pattern 'ACYCLIC (n1)-[:R]->{2,2}(n2)' is used, because that is already larger than 'allNodes * allNodes'
      cardinalityChain2Rels3Nodes
    )
  }

  test("SHORTEST 1 ACYCLIC GROUP (n1)-[:R]->{2,5}(n2)") {
    queryShouldHaveCardinality(
      planner.build(),
      CypherVersion.Cypher25,
      "MATCH SHORTEST 1 ACYCLIC GROUP (n1)-[:R]->{2,5}(n2)",
      // The cardinality estimate of the pattern 'ACYCLIC (n1)-[:R]->{2,5}(n2)' is used, because all {2,2}, (2,3), {2,4} and {2,5} are lower than 'allNodes * allNodes'
      cardinalityChainWithNRelsWithLabelR(2)
        + cardinalityChainWithNRelsWithLabelR(3)
        + cardinalityChainWithNRelsWithLabelR(4)
        + cardinalityChainWithNRelsWithLabelR(5)
    )
  }
}
