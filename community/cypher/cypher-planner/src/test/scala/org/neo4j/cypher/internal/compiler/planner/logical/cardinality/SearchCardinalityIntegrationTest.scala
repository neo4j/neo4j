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
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.VectorSearchWithComplexPattern
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.util.Selectivity

class SearchCardinalityIntegrationTest extends CypherPlannerTestSuite with CardinalityIntegrationTestSupport {
  private val allNodes: Double = 1000.0
  private val allRels: Double = 50000.0
  private val `()-[:ACTED_IN]->()`: Double = 540.0
  private val `()-[:DIRECTED]->()`: Double = 15.0
  private val `(:Movie)`: Double = 200.0
  private val `(:Book)`: Double = 120.0
  private val `()-[:ACTED_IN]->(:Movie)`: Double = 330.0
  private val `()-[:ACTED_IN]->(:Book)`: Double = 130.0

  private val planner =
    plannerBuilder()
      .addSemanticFeature(VectorSearchWithComplexPattern)
      .setAllNodesCardinality(allNodes)
      .setAllRelationshipsCardinality(allRels)
      .setLabelCardinality("Movie", `(:Movie)`)
      .setLabelCardinality("Book", `(:Book)`)
      .setRelationshipCardinality("()-[:ACTED_IN]->()", `()-[:ACTED_IN]->()`)
      .setRelationshipCardinality("()-[:DIRECTED]->()", `()-[:DIRECTED]->()`)
      .setRelationshipCardinality("()-[:ACTED_IN]->(:Movie)", `()-[:ACTED_IN]->(:Movie)`)
      .setRelationshipCardinality("()-[:ACTED_IN]->(:Book)", `()-[:ACTED_IN]->(:Book)`)
      .addNodeVectorIndex("moviePlots", Seq("Movie"), "plot", Seq("imdbRating", "releaseYear"))
      .addNodeVectorIndex("movieOrBookPlots", Seq("Movie", "Book"), "plot", Seq("imdbRating", "releaseYear"))
      .addRelationshipVectorIndex("actsInScript", Seq("ACTED_IN"), "script", Seq("workingDays"))
      .addRelationshipVectorIndex("actsInOrDirectsScript", Seq("ACTED_IN", "DIRECTED"), "script", Seq("workingDays"))

  test(
    "Node vector index SEARCH should produce cardinality estimate equal to the limit of the SEARCH when the limit is less than the cardinality of the index pattern"
  ) {
    Seq(1, 10, 20, `(:Movie)`.toInt - 1).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (n:Movie)
           |SEARCH n IN (
           |  VECTOR INDEX moviePlots
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        limit // `(:Movie)` * (limit / `(:Movie)`)
      )
    )
  }

  test(
    "Node vector index SEARCH should produce cardinality estimate equal to the cardinality of the index pattern when the limit of the SEARCH is larger"
  ) {
    val ipc = `(:Movie)`.toInt // Index Pattern Cardinality
    Seq(ipc, ipc + 1, ipc + 10, ipc + 3000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (n:Movie)
           |SEARCH n IN (
           |  VECTOR INDEX moviePlots
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        `(:Movie)`
      )
    )
  }

  test(
    "Node vector index SEARCH cardinality is DEFAULT (DEFAULT_LIMIT_ROW_COUNT) when limit is parameter and index cardinality > DEFAULT"
  ) {
    queryShouldHaveCardinality(
      planner
        .build(),
      CypherVersion.Cypher25,
      """MATCH (n:Movie)
        |SEARCH n IN (
        |  VECTOR INDEX moviePlots
        |  FOR [1, 2, 3]
        |  LIMIT $limit
        |)
        |""".stripMargin,
      PlannerDefaults.DEFAULT_LIMIT_ROW_COUNT // `(:Movie)` * Math.min(PlannerDefaults.DEFAULT_LIMIT_ROW_COUNT / `(:Movie)`, 1)
    )
  }

  test(
    "Node vector index SEARCH should produce cardinality estimate equal to the index cardinality when the limit in the SEARCH is specified using a parameter and the index pattern cardinality is less than the DEFAULT planner limit"
  ) {
    val `(:A)` = 5
    queryShouldHaveCardinality(
      planner
        .setLabelCardinality("A", `(:A)`)
        .addNodeVectorIndex("aPlots", Seq("A"), "plot")
        .build(),
      CypherVersion.Cypher25,
      """MATCH (n:A)
        |SEARCH n IN (
        |  VECTOR INDEX aPlots
        |  FOR [1, 2, 3]
        |  LIMIT $limit
        |)
        |""".stripMargin,
      `(:A)` // `(:A)` * Math.min(PlannerDefaults.DEFAULT_LIMIT_ROW_COUNT / `(:A)`, 1)
    )
  }

  test(
    "Node vector index SEARCH selectivity, when the label of the index was not specified on the bound variable in the MATCH"
  ) {
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (n)
           |SEARCH n IN (
           |  VECTOR INDEX moviePlots
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        // The SEARCH implies the label `Movie` on node `n`
        Math.min(limit, `(:Movie)`)
      )
    )
  }

  test(
    "Relationship vector index SEARCH selectivity, when the type on relationship variable is not specified"
  ) {
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH ()-[r]->()
           |SEARCH r IN (
           |  VECTOR INDEX actsInScript
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        Math.min(limit, `()-[:ACTED_IN]->()`)
      )
    )
  }

  test(
    "Relationship vector index SEARCH selectivity, when the type on relationship variable is specified"
  ) {
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH ()-[r:ACTED_IN]->()
           |SEARCH r IN (
           |  VECTOR INDEX actsInScript
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        Math.min(limit, `()-[:ACTED_IN]->()`)
      )
    )
  }

  test(
    "Relationship vector index SEARCH selectivity, when the type on relationship variable is specified in the WHERE clause"
  ) {
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH ()-[r]->()
           |WHERE r:ACTED_IN
           |SEARCH r IN (
           |  VECTOR INDEX actsInScript
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        Math.min(limit, `()-[:ACTED_IN]->()`)
      )
    )
  }

  test(
    "Case with only different relationship type than the type of the relationship search index"
  ) {
    val `()-[:R]->()` = 500
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .setRelationshipCardinality("()-[:R]->()", `()-[:R]->()`)
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH ()-[r:R]->()
           |SEARCH r IN (
           |  VECTOR INDEX actsInScript
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        // Cardinality should be 0, since r cannot have both the type R and ACTED_IN.
        0
      )
    )
  }

  test(
    "Case with different relationship type than the type of the relationship search index"
  ) {
    val `()-[:R]->()` = 500
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .setRelationshipCardinality("()-[:R]->()", `()-[:R]->()`)
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH ()-[r:R|ACTED_IN]->()
           |SEARCH r IN (
           |  VECTOR INDEX actsInScript
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        Math.min(limit, `()-[:ACTED_IN]->()`)
      )
    )
  }

  test(
    "Case with different relationship type (as predicates in WHERE clause) than the type of the relationship search index"
  ) {
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .setRelationshipCardinality("()-[:R]->()", 500)
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH ()-[r]->()
           |WHERE r:R
           |SEARCH r IN (
           |  VECTOR INDEX actsInScript
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        // Relationship r cannot have types R and ACTED_IN
        0
      )
    )
  }

  test(
    "Case with different relationship type (as predicates in WHERE clause) than the type of the relationship search index - index relType also included in WHERE"
  ) {
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .setRelationshipCardinality("()-[:R]->()", 500)
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH ()-[r]->()
           |WHERE r:ACTED_IN AND r:R
           |SEARCH r IN (
           |  VECTOR INDEX actsInScript
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        // Should return cardinality of 0, since r cannot have both types R and ACTED_IN
        0
      )
    )
  }

  test("For each movie, find n movies with the most similar plots") {
    Seq(1, 10, 20, 300, 5000).foreach(n =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (x:Movie)
           |MATCH (m)
           |SEARCH m IN (
           |  VECTOR INDEX moviePlots
           |  FOR x.plot
           |  LIMIT $n
           |)
           |""".stripMargin,
        `(:Movie)` * Math.min(n, `(:Movie)`)
      )
    )
  }

  test(
    "For each movie, select it if its has one of the n most similar plots to vector [1,2,3] - the bound node of the vector search was already bound before"
  ) {
    Seq(1, 10, 20, 300, 5000).foreach(n =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (m:Movie)
           |MATCH (m)
           |SEARCH m IN (
           |  VECTOR INDEX moviePlots
           |  FOR [1,2,3]
           |  LIMIT $n
           |)
           |""".stripMargin,
        `(:Movie)` * Math.min(n / `(:Movie)`, 1)
      )
    )
  }

  test("Node vector index SEARCH with MATCH (n:Movie)<-[e:ACTED_IN]-(a) SEARCH n") {
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (n:Movie)<-[e:ACTED_IN]-(a)
           |SEARCH n IN (
           |  VECTOR INDEX moviePlots
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        `()-[:ACTED_IN]->(:Movie)` * Math.min(limit / `(:Movie)`, 1)
      )
    )
  }

  test("Node vector index SEARCH with MATCH (n)<-[e:ACTED_IN]-(a) SEARCH n") {
    // The SEARCH only retrieves nodes with the label :Movie due to the construction of the index moviePlots
    Seq(1, 10, 20, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (n)<-[e:ACTED_IN]-(a)
           |SEARCH n IN (
           |  VECTOR INDEX moviePlots
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        `()-[:ACTED_IN]->(:Movie)` * Math.min(limit / `(:Movie)`, 1)
      )
    )
  }

  test("Node vector index SEARCH with multiple disjunctive labels") {
    val `(:Movie|Book)` = IndependenceCombiner.orTogetherSelectivities(Seq(
      Selectivity(`(:Movie)` / allNodes),
      Selectivity(`(:Book)` / allNodes)
    )).get.factor * allNodes
    Seq(1, 10, 20, 100, 150, 200, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (n)
           |SEARCH n IN (
           |  VECTOR INDEX movieOrBookPlots
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        Math.min(limit, `(:Movie|Book)`)
      )
    )
  }

  test("Node vector index SEARCH with multiple disjunctive labels - expand the pattern with <-[:ACTED_IN]-") {
    val `(:Movie|Book)` = IndependenceCombiner.orTogetherSelectivities(Seq(
      Selectivity(`(:Movie)` / allNodes),
      Selectivity(`(:Book)` / allNodes)
    )).get.factor * allNodes
    val `(:Move|Book)<-[:ACTED_IN]-()` = IndependenceCombiner.orTogetherSelectivities(Seq(
      Selectivity(`()-[:ACTED_IN]->(:Movie)` / `()-[:ACTED_IN]->()`),
      Selectivity(`()-[:ACTED_IN]->(:Book)` / `()-[:ACTED_IN]->()`)
    )).get.factor * `()-[:ACTED_IN]->()`
    Seq(1, 10, 20, 100, 150, 200, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (n)<-[e:ACTED_IN]-(a)
           |SEARCH n IN (
           |  VECTOR INDEX movieOrBookPlots
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        `(:Move|Book)<-[:ACTED_IN]-()` * Math.min(limit / `(:Movie|Book)`, 1)
      )
    )
  }

  test("Relationship vector index SEARCH with multiple disjunctive types") {
    Seq(1, 10, 20, 100, 150, 200, 300, 5000).foreach(limit =>
      queryShouldHaveCardinality(
        planner
          .build(),
        CypherVersion.Cypher25,
        s"""MATCH (n)<-[e]-(a)
           |SEARCH e IN (
           |  VECTOR INDEX actsInOrDirectsScript
           |  FOR [1, 2, 3]
           |  LIMIT $limit
           |)
           |""".stripMargin,
        Math.min(limit, `()-[:ACTED_IN]->()` + `()-[:DIRECTED]->()`)
      )
    )
  }
}
