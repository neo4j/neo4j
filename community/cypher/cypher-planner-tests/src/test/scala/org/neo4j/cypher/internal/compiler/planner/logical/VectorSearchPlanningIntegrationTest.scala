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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.VectorSearchWithComplexPattern
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.plans.AllQueryExpression
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.NonExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.exceptions.IndexSearchException

class VectorSearchPlanningIntegrationTest
    extends VectorSearchPlanningIntegrationTestBase

class VectorSearchWithComplexPatternPlanningIntegrationTest
    extends VectorSearchWithComplexPatternPlanningIntegrationTestBase

abstract class VectorSearchPlanningIntegrationTestBase extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanConstructionTestSupport
    with QueryExpressionConstructionTestSupport
    with LogicalPlanningAttributesTestSupport {

  protected def movieLabelCardinality: Double = 80.0

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .setAllNodesCardinality(120)
      .setLabelCardinality("Movie", movieLabelCardinality)
      .setLabelCardinality("Actor", 40)
      .setLabelCardinality("Person", 10)
      .setLabelCardinality("Director", 5)
      .setRelationshipCardinality("()-[]->()", 100)
      .setRelationshipCardinality("()-[:ACTS_IN]->()", 50)
      .setRelationshipCardinality("(:Person)-[:ACTS_IN]->(:Movie)", 50)
      .setRelationshipCardinality("(:Person)-[:ACTS_IN]->()", 50)
      .setRelationshipCardinality("()-[:ACTS_IN]->(:Movie)", 50)
      .setRelationshipCardinality("()-[:CONTRIBUTED]->()", 8)
      .setRelationshipCardinality("()-[:CONTRIBUTED]->(:Movie)", 7)
      .setRelationshipCardinality("(:Movie)-[]->()", 20)
      .setRelationshipCardinality("()-[:DIRECTED]->()", 20)
      .setRelationshipCardinality("(:Person)-[:DIRECTED]->()", 20)
      .setRelationshipCardinality("()-[:DIRECTED]->(:Movie)", 20)
      .setRelationshipCardinality("(:Person)-[:DIRECTED]->(:Movie)", 20)
      // node indexes
      .addNodeVectorIndex("moviePlots", Seq("Movie"), "plot", Seq("imdbRating", "releaseYear"))
      .addNodeVectorIndex("movieOrDirectorInfo", Seq("Movie", "Director"), "info")
      .addNodeIndex("Movie", List("title"), 1.0, 1.0 / 120.0, isUnique = true)
      // relationship indexes
      .addRelationshipVectorIndex("actsInScript", Seq("ACTS_IN"), "script", Seq("workingDays"))
      .addRelationshipVectorIndex("contributed", Seq("CONTRIBUTED"), "embedding")
      .addRelationshipVectorIndex("actsOrContributedInScript", Seq("ACTS_IN", "CONTRIBUTED"), "script")

  protected val moviePlotsProperties: Seq[String] = Seq("plot", "imdbRating", "releaseYear")
  protected val actsInScriptProperties: Seq[String] = Seq("script", "workingDays")

  test("plan node vector index search") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) WHERE movie.plot IS NOT NULL
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`")
        .projection("movie.plot AS `movie.plot`")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test("node vector index search must not claim an additional (filter) property value from the index") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie.imdbRating""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.imdbRating`")
        .projection("movie.imdbRating AS `movie.imdbRating`")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = _ => DoNotGetValue
        )
        .build()
  }

  test("should plan vector index operator with all labels in the correct order") {
    val planner =
      plannerBuilder()
        // This hopefully mixes up the order of label ids
        .setLabelCardinality("C", 5)
        .setLabelCardinality("D", 5)
        .setLabelCardinality("E", 5)
        .setLabelCardinality("F", 5)
        .setLabelCardinality("G", 5)
        .setLabelCardinality("H", 5)
        .setLabelCardinality("I", 5)
        .setLabelCardinality("J", 5)
        .setLabelCardinality("A", 5)
        .setLabelCardinality("B", 5)
        .addNodeVectorIndex("lotsOfLabels", Seq("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"), "prop")
        .build()

    val query =
      """MATCH (n)
        |  SEARCH n IN (
        |    VECTOR INDEX lotsOfLabels
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN n.prop""".stripMargin

    planner.plan(CypherVersion.Cypher25, query) should equal(
      planner.planBuilder()
        .produceResults("`n.prop`")
        .projection("n.prop AS `n.prop`")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
          properties = Seq("prop"),
          indexName = "lotsOfLabels",
          vector = "$embedding",
          limit = "10"
        )
        .build()
    )
  }

  test(
    "A plan using a node vector index search with multiple labels should provide all Labels and filter on the one specified."
  ) {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) WHERE movie.info IS NOT NULL
        |  SEARCH movie IN (
        |    VECTOR INDEX movieOrDirectorInfo
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie.info""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.info`")
        .projection("movie.info AS `movie.info`")
        .filter(
          "movie:Movie"
        ) // This filter is important since we cannot infer that the index returns a node with the Movie Label.
        .nodeVectorIndexSearch(
          node = "movie",
          Seq("Movie", "Director"),
          Seq("info"),
          "movieOrDirectorInfo",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test("plan node vector index search as part of a longer path pattern") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)-->()
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie")
        .expandAll("(movie)-[]->()")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10"
        )
        .build()
  }

  test("plan node vector index search taking the embedding as an argument") {
    val planner = plannerBuilder().build()

    val query =
      """WITH [1,2,3,4,5] AS embedding
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR embedding
        |    LIMIT 10
        |  )
        |RETURN movie""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "embedding",
          limit = "10",
          argumentIds = Set("embedding")
        )
        .projection("[1, 2, 3, 4, 5] AS embedding")
        .argument()
        .build()
  }

  test("plan node vector index search where the embedding contains a collect sub-query") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR COLLECT { MATCH (m:Movie) RETURN m.releaseYear ORDER BY m.releaseYear }
        |    LIMIT 10
        |  )
        |RETURN movie""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "anon_0",
          limit = "10",
          argumentIds = Set("anon_0")
        )
        .rollUpApply("anon_0", "m.releaseYear")
        .|.sort("`m.releaseYear` ASC")
        .|.projection("m.releaseYear AS `m.releaseYear`")
        .|.nodeByLabelScan("m", "Movie")
        .argument()
        .build()
  }

  test("plan node vector index search, taking limit as an argument") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT $limit
        |  )
        |RETURN movie""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "$limit"
        )
        .build()
  }

  test("plan node vector index search projecting the score variable") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  ) SCORE as similarity
        |RETURN movie, similarity""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie", "similarity")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          score = "similarity",
          argumentIds = Set()
        )
        .build()
  }

  test("should plan score filter in a query with multiple MATCH clauses") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  ) SCORE AS similarity
        |WHERE similarity > 0.8
        |MATCH (node)
        |RETURN movie.plot, similarity, node""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.planBuilder()
      .produceResults("`movie.plot`", "similarity", "node")
      .projection("cacheN[movie.plot] AS `movie.plot`")
      .apply()
      .|.allNodeScan("node", "movie", "similarity")
      .cacheProperties("cacheNFromStore[movie.plot]")
      .filter("similarity > 0.8")
      .nodeVectorIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = Seq("plot", "imdbRating", "releaseYear"),
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        score = "similarity",
        argumentIds = Set()
      )
      .build()
  }

  // TODO: see Example SQ2 in CIP-224
  //  See PLAN-3087
  ignore("plan node vector index search, where the embedding refers to the binding variable") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR movie.titleEmbedding
        |    LIMIT 10
        |  )
        |  WHERE movie.title IN $titles
        |RETURN movie""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie")
        .filter("`  movie@0` = movie")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "  movie@0",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "movie.titleEmbedding",
          limit = "10",
          argumentIds = Set("movie")
        )
        .filter("movie.title IN $titles")
        .allNodeScan("movie")
        .build()
  }

  test("should not get the property value from the index when it is not needed afterwards") {
    val planner = plannerBuilder().build()
    // Vector index moviePlots is on (label: "Movie", property: "plot")
    // The property "plot" is not needed in the rest of the query
    val query =
      """MATCH (movie:Movie) WHERE movie.p1 IS NOT NULL
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie.p1""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.p1`")
        .projection("cacheN[movie.p1] AS `movie.p1`")
        .filter("cacheNFromStore[movie.p1] IS NOT NULL")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10"
        )
        .build()
  }

  test("should remove implicit disjunctions") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) WHERE movie.plot IS NOT NULL OR movie.plot2 = 'someValue'
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`")
        .projection("movie.plot AS `movie.plot`")
        // movie.plot IS NOT NULL is solved by vector search implicitly so we don't need to check movie.plot2 = 'someValue'
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test(
    "plan node vector index search using a relationship variable as binding variable should give GQL error 22G03 with cause 22N01"
  ) {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r:ACTS_IN]->()
        |  SEARCH r IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r""".stripMargin

    val caughtException = intercept[IndexSearchException] {
      planner.plan(CypherVersion.Cypher25, query)
    }
    caughtException.gqlStatus() should be("22G03")
    caughtException.statusDescription() should be("error: data exception - invalid value type")
    caughtException.legacyMessage() should be(
      "22N01: Expected the value `r` to be of type NODE, but was of type RELATIONSHIP."
    )

    val caughtExceptionCause = caughtException.cause()
    caughtExceptionCause.isEmpty should be(false)
    caughtExceptionCause.get.gqlStatus() should be("22N01")
    caughtExceptionCause.get.statusDescription() should be(
      "error: data exception - invalid type. Expected the value `r` to be of type NODE, but was of type RELATIONSHIP."
    )
    caughtExceptionCause.get.cause().isEmpty should be(true)
  }

  test(
    "plan relationship vector index search using a node variable as binding variable should give GQL error 22G03 with cause 22N01"
  ) {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (r:Movie)
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r""".stripMargin

    val caughtException = intercept[IndexSearchException] {
      planner.plan(CypherVersion.Cypher25, query)
    }
    caughtException.gqlStatus() should be("22G03")
    caughtException.statusDescription() should be("error: data exception - invalid value type")
    caughtException.legacyMessage() should be(
      "22N01: Expected the value `r` to be of type RELATIONSHIP, but was of type NODE."
    )

    val caughtExceptionCause = caughtException.cause()
    caughtExceptionCause.isEmpty should be(false)
    caughtExceptionCause.get.gqlStatus() should be("22N01")
    caughtExceptionCause.get.statusDescription() should be(
      "error: data exception - invalid type. Expected the value `r` to be of type RELATIONSHIP, but was of type NODE."
    )
    caughtExceptionCause.get.cause().isEmpty should be(true)
  }

  test(
    "plan node vector index search using a node variable (hidden from the planner) passed as a relationship variable should give GQL error 22G03 with cause 22N01"
  ) {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (n)-[r]->()
        |WITH coalesce(n, r) as x
        |MATCH ()-[x]->()
        | SEARCH x IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN x.prop""".stripMargin

    val caughtException = intercept[IndexSearchException] {
      planner.plan(CypherVersion.Cypher25, query)
    }
    caughtException.gqlStatus() should be("22G03")
    caughtException.statusDescription() should be("error: data exception - invalid value type")
    caughtException.legacyMessage() should be(
      "22N01: Expected the value `x` to be of type NODE, but was of type RELATIONSHIP."
    )

    val caughtExceptionCause = caughtException.cause()
    caughtExceptionCause.isEmpty should be(false)
    caughtExceptionCause.get.gqlStatus() should be("22N01")
    caughtExceptionCause.get.statusDescription() should be(
      "error: data exception - invalid type. Expected the value `x` to be of type NODE, but was of type RELATIONSHIP."
    )
    caughtExceptionCause.get.cause().isEmpty should be(true)
  }

  test("should fail with an index-in-populating-state error when the vector index is populating") {
    val planner =
      plannerBuilder()
        .addNodeVectorIndex("populatingMovieEmbeddings", Seq("Movie"), "embedding")
        .addPopulatingIndex("populatingMovieEmbeddings")
        .build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX populatingMovieEmbeddings
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie""".stripMargin

    val caughtException = intercept[IndexSearchException] {
      planner.plan(CypherVersion.Cypher25, query)
    }
    caughtException.gqlStatus() should be("51N63")
    caughtException.legacyMessage() should be(
      "51N63: Index `populatingMovieEmbeddings` is not ready yet. Wait until it finishes populating and retry the transaction."
    )
  }

  test("relationship vector index search must not claim the returned property value from the index") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.script""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.script`")
        .projection("r.script AS `r.script`")
        .relationshipVectorIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = _ => DoNotGetValue
        )
        .build()
  }

  test("relationship vector index search must not claim an additional (filter) property value from the index") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.workingDays""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.workingDays`")
        .projection("r.workingDays AS `r.workingDays`")
        .relationshipVectorIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = _ => DoNotGetValue
        )
        .build()
  }

  test("plan relationship vector index search") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .relationshipVectorIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = Map("script" -> DoNotGetValue, "workingDays" -> DoNotGetValue)
        )
        .build()
  }

  test("plan relationship vector index search should show all indexed Types") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->()
        |  SEARCH r IN (
        |    VECTOR INDEX actsOrContributedInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .relationshipVectorIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN", "CONTRIBUTED"),
          properties = Seq("script"),
          indexName = "actsOrContributedInScript",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = Map("script" -> DoNotGetValue)
        )
        .build()
  }

  test("plan relationship vector index search and get and cache property value from index") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("r")
        .relationshipVectorIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test("plan undirected relationship vector index search and get and cache property value from index") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]-()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("r")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test("plan incoming directed relationship vector index search") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()<-[r]-()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .relationshipVectorIndexSearch(
          pattern = "()<-[r]-()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = Map("script" -> DoNotGetValue, "workingDays" -> DoNotGetValue)
        )
        .build()
  }

  test("plan undirected relationship vector index search") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]-()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .relationshipVectorIndexSearch(
          pattern = "()-[r]-()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = Map("script" -> DoNotGetValue, "workingDays" -> DoNotGetValue)
        )
        .build()
  }

  test("identify implicitly solved predicates when planning relationship vector index search") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r:ACTS_IN]-()
        |  WHERE r.script IS NOT NULL
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .relationshipVectorIndexSearch(
          pattern = "()-[r]-()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = Map("script" -> DoNotGetValue, "workingDays" -> DoNotGetValue)
        )
        .build()
  }

  test(
    "identify implicit type predicates within conjunctions and ensure plan filters on the non-implicit predicates"
  ) {
    val planner = plannerBuilder().build()

    // Since a relationship can only have one type, the filter on r:CONTRIBUTED should ensure that this query produces no results.
    // The filter on r:ACTS_IN is implicit in the vector index search and need not be planned (even if it is in the WHERE clause).
    val query =
      """MATCH ()-[r]-()
        |  WHERE r:ACTS_IN AND r:CONTRIBUTED
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .filter(hasTypes("r", "CONTRIBUTED"))
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          Seq("ACTS_IN"),
          actsInScriptProperties,
          "actsInScript",
          "$embedding",
          "10"
        )
        .build()
  }

  test(
    "identify that there are no implicit type predicates within conjunctions on multi type vector indexes and ensure plan filters on the non-implicit predicates"
  ) {
    val planner = plannerBuilder().build()

    // Since a relationship can only have one type, the filter on r:CONTRIBUTED should ensure that this query produces no results.
    // The filter on r:ACTS_IN is NOT implicit in the vector index search and need to be planned.
    val query =
      """MATCH ()-[r]-()
        |  WHERE r:ACTS_IN AND r:CONTRIBUTED
        |  SEARCH r IN (
        |    VECTOR INDEX actsOrContributedInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .filter(andsReorderable("r:ACTS_IN", "r:CONTRIBUTED"))
        .relationshipVectorIndexSearch(
          pattern = "()-[r]-()",
          typeNames = Seq("ACTS_IN", "CONTRIBUTED"),
          properties = Seq("script"),
          indexName = "actsOrContributedInScript",
          vector = "$embedding",
          limit = "10"
        )
        .build()
  }

  test("plan relationship vector index on RHS of apply - using WITH r SKIP 0") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (:Movie {title: "Matrix"})<-[r:CONTRIBUTED]-()
        |WITH r SKIP 0
        |MATCH ()-[c]->()
        |  SEARCH c IN (
        |    VECTOR INDEX contributed
        |    FOR r.embedding
        |    LIMIT 5
        |  )
        |RETURN c.description AS contributionDesc""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("contributionDesc")
        .projection("c.description AS contributionDesc")
        .apply()
        .|.relationshipVectorIndexSearch(
          pattern = "()-[c]->()",
          typeNames = Seq("CONTRIBUTED"),
          properties = Seq("embedding"),
          indexName = "contributed",
          vector = "r.embedding",
          limit = "5",
          argumentIds = Set("r")
        )
        .skip(0)
        .expandAll("(anon_0)<-[r:CONTRIBUTED]-()")
        .nodeIndexOperator("anon_0:Movie(title = 'Matrix')", unique = true)
        .build()
  }

  // TODO see PLAN-2843
  test("plan relationship vector index on RHS of apply") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (:Movie {title: "Matrix"})<-[r:CONTRIBUTED]-()
        |MATCH ()-[c]->()
        |  SEARCH c IN (
        |    VECTOR INDEX contributed
        |    FOR r.embedding
        |    LIMIT 5
        |  )
        |RETURN c.description AS contributionDesc""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("contributionDesc")
        .projection("c.description AS contributionDesc")
        .apply()
        .|.relationshipVectorIndexSearch(
          pattern = "()-[c]->()",
          typeNames = Seq("CONTRIBUTED"),
          properties = Seq("embedding"),
          indexName = "contributed",
          vector = "r.embedding",
          limit = "5",
          argumentIds = Set("anon_0", "anon_1", "r")
        )
        .expandAll("(anon_0)<-[r:CONTRIBUTED]-(anon_1)")
        .nodeIndexOperator("anon_0:Movie(title = 'Matrix')", unique = true)
        .build()
  }

  test("plan relationship vector index search with predicates on relationship") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->() WHERE r.prop > 50 and r.prop2 CONTAINS "running"
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .filter("r.prop2 CONTAINS 'running'", "r.prop > 50")
        .relationshipVectorIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = Map("script" -> DoNotGetValue, "workingDays" -> DoNotGetValue)
        )
        .build()
  }

  test("plan vector index using another variable in the embedding") {
    // Currently cartesian products do not allow vector search to be planned later
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 120)
      .build()

    val query =
      """MATCH (person:Person)
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR person.embedding
        |    LIMIT 10
        |  )
        |RETURN movie, person""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie", "person")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "person.embedding",
          limit = "10",
          argumentIds = Set("person")
        )
        .nodeByLabelScan("person", "Person")
        .build()
  }

  test("should support using a returned score in subsequent predicates") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) WHERE movie.plot IS NOT NULL
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  ) SCORE AS similarity
        |WITH movie, similarity
        |WHERE similarity > 0.8
        |RETURN movie.plot, similarity""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`", "similarity")
        .projection("movie.plot AS `movie.plot`")
        .filter("similarity > 0.8")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          score = "similarity"
        )
        .build()
  }

  test("should not use the countStore to count when using vector search") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  ) SCORE AS similarity
        |RETURN count(*), similarity""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`count(*)`", "similarity")
        .aggregation(Seq("similarity AS similarity"), Seq("count(*) AS `count(*)`"))
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          score = "similarity",
          argumentIds = Set()
        )
        .build()
  }

  test("should use getDegree when using vector search") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  WHERE NOT (movie)-[]->()
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie")
        .filter(not(hasDegreeGreater("movie", OUTGOING, literalInt(0))))
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  // TODO update apply to join after PLAN-3088
  test("plan node vector index search for previously bound nodes") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """
        |MATCH (d:Person {name: "Pakula, Alan"})-[:DIRECTED]->(movie:Movie)
        |MATCH (movie:Movie)
        | SEARCH movie IN (
        |   VECTOR INDEX moviePlots
        |   FOR $embedding
        |   LIMIT 5
        | )
        |RETURN movie.title AS title
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("title")
        .projection("movie.title AS title")
        .filter("`  movie@0` = movie", assertIsNode("movie"))
        .apply()
        .|.nodeVectorIndexSearch(
          node = "  movie@0",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "5",
          argumentIds = Set("d", "movie", "  UNNAMED0")
        )
        .filter("movie:Movie")
        .expandAll("(d)-[`  UNNAMED0`:DIRECTED]->(movie)")
        .filter("d.name = 'Pakula, Alan'")
        .nodeByLabelScan("d", "Person")
        .build()
  }

  // TODO update apply to Cartesian product after PLAN-3088
  test("plan multiple node vector index searches in different horizons") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) WHERE movie.plot IS NOT NULL
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |MATCH (movie2:Movie) WHERE movie2.plot IS NOT NULL
        |  SEARCH movie2 IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie.plot, movie2.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`", "`movie2.plot`")
        .projection("cacheN[movie.plot] AS `movie.plot`", "movie2.plot AS `movie2.plot`")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie2",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set("movie")
        )
        .cacheProperties("cacheNFromStore[movie.plot]")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10"
        )
        .build()
  }

  // TODO update apply to Cartesian product after PLAN-3088
  test("plan a MATCH-SEARCH with another MATCH with no overlapping entities") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (person:Person)
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN person.name, movie.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`person.name`", "`movie.plot`")
        .projection("cacheN[person.name] AS `person.name`", "movie.plot AS `movie.plot`")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set("person")
        )
        .cacheProperties("cacheNFromStore[person.name]")
        .nodeByLabelScan("person", "Person")
        .build()
  }

  // TODO update apply to join after PLAN-3088
  test("plan multiple node vector index searches where the first match supplies the embedding for the second") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) WHERE movie.plot IS NOT NULL
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |MATCH (movie2:Movie) WHERE movie2.plot IS NOT NULL
        |  SEARCH movie2 IN (
        |    VECTOR INDEX moviePlots
        |    FOR movie.embedding
        |    LIMIT 10
        |  )
        |RETURN movie.plot, movie2.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`", "`movie2.plot`")
        .projection("cacheN[movie.plot] AS `movie.plot`", "movie2.plot AS `movie2.plot`")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie2",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "movie.embedding",
          limit = "10",
          argumentIds = Set("movie")
        )
        .cacheProperties("cacheNFromStore[movie.plot]")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test("plan relationship vector index search with previously bound relationship") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """
        |MATCH (d:Person {name: "Beatty, Warren"})-[a:ACTS_IN]->(movie:Movie)
        |MATCH ()-[a:ACTS_IN]->()
        |  SEARCH a IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  ) SCORE AS similarity
        |RETURN a.p AS p, similarity
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("p", "similarity")
        .projection("a.p AS p")
        .filter("`  a@0` = a")
        .apply()
        .|.relationshipVectorIndexSearch(
          pattern = "()-[`  a@0`]->()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          score = "similarity",
          argumentIds = Set("d", "movie", "a"),
          getValueFromIndex = Map("script" -> DoNotGetValue, "workingDays" -> DoNotGetValue)
        )
        .filter("movie:Movie")
        .expandAll("(d)-[a:ACTS_IN]->(movie)")
        .filter("d.name = 'Beatty, Warren'")
        .nodeByLabelScan("d", "Person")
        .build()
  }

  test("should check for type when planning RelationshipVectorIndexSearch") {
    val planner = plannerBuilder().build()
    // Since a relationship can only have one type, the filter on r:CONTRIBUTED expects r to have type contributed.
    // The filter on r:ACTS_IN is implicit in the vector index search.
    // This should result in an empty result set, and the generated plan should reflect that by planning the filter.
    val query =
      """MATCH ()-[r:CONTRIBUTED]->()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    val expectedPlan =
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .filter(hasTypes("r", "CONTRIBUTED"))
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          Seq("ACTS_IN"),
          actsInScriptProperties,
          "actsInScript",
          "$embedding",
          "10"
        )
        .build()

    plan shouldEqual expectedPlan
  }

  test(
    "should ignore unsolved relationship type in a disjunction if RelationshipVectorIndexSearch implies the other type"
  ) {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r:CONTRIBUTED|ACTS_IN]->()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          Seq("ACTS_IN"),
          actsInScriptProperties,
          "actsInScript",
          "$embedding",
          "10"
        )
        .build()
  }

  private val valuesToCompare = Seq(
    ("8", literalInt(8)),
    ("$parameter", parameter("parameter", CTAny))
  )

  private val comparisons: Seq[(String, QueryExpression[Expression])] = valuesToCompare.flatMap {
    case (cypher, expression) =>
      Seq(
        (s"IN [ $cypher ]", QueryExpressionConstructionTestSupport.single(expression)),
        (s"= $cypher", QueryExpressionConstructionTestSupport.single(expression)),
        (s"< $cypher", rangeExpression(lt(expression))),
        (s"<= $cypher", rangeExpression(lte(expression))),
        (s"> $cypher", rangeExpression(gt(expression))),
        (s">= $cypher", rangeExpression(gte(expression)))
      )
  } ++ Seq(
    ("IS NOT NULL", ExistenceQueryExpression),
    ("IS NULL", NonExistenceQueryExpression)
  )

  test("plan node vector search with inlined predicate") {
    val planner =
      plannerBuilder()
        .build()

    for ((comparison, comparisonExpression) <- comparisons) {
      withClue(comparison) {
        val query =
          s"""MATCH (movie:Movie)
             |  SEARCH movie IN (
             |    VECTOR INDEX moviePlots
             |    FOR $$embedding
             |    WHERE movie.imdbRating $comparison
             |    LIMIT 5
             |  )
             |RETURN movie""".stripMargin

        planner.plan(CypherVersion.Cypher25, query) should equal(
          planner.planBuilder()
            .produceResults("movie")
            .nodeVectorIndexSearch(
              node = "movie",
              labelNames = Seq("Movie"),
              properties = moviePlotsProperties,
              indexName = "moviePlots",
              vector = "$embedding",
              limit = "5",
              propertyFilter = Some(composite(comparisonExpression, AllQueryExpression))
            )
            .build()
        )
      }
    }
  }

  test("plan node vector search with inlined between predicate") {
    val planner =
      plannerBuilder()
        .build()

    val query =
      s"""MATCH (movie:Movie)
         |  SEARCH movie IN (
         |    VECTOR INDEX moviePlots
         |    FOR $$embedding
         |    WHERE movie.imdbRating < $$upperBound AND movie.imdbRating >= $$lowerBound AND movie.releaseYear = 1989
         |    LIMIT 5
         |  )
         |RETURN movie""".stripMargin

    planner.plan(CypherVersion.Cypher25, query) should equal(
      planner.planBuilder()
        .produceResults("movie")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "5",
          propertyFilter = Some(composite(
            between(
              gte(parameter("lowerBound", CTAny)),
              lt(parameter("upperBound", CTAny))
            ),
            single(literalInt(1989))
          ))
        )
        .build()
    )
  }

  test("planning node vector search with inlined predicate should fail gracefully if the property is not indexed") {
    val planner =
      plannerBuilder()
        .build()

    for ((comparison, _) <- comparisons) {
      withClue(comparison) {
        val query =
          s"""MATCH (movie:Movie)
             |  SEARCH movie IN (
             |    VECTOR INDEX moviePlots
             |    FOR $$embedding
             |    WHERE movie.imdbVotes $comparison
             |    LIMIT 5
             |  )
             |RETURN movie""".stripMargin

        val exception =
          the[IndexSearchException] thrownBy {
            planner.plan(CypherVersion.Cypher25, query)
          }

        exception should not equal null
        exception.getMessage should equal(
          "22ND3: The property `imdbVotes` is not an additional property for vector search with filters on the vector index `moviePlots`."
        )
      }
    }
  }

  test("plan relationship vector search with inlined predicate") {
    val planner =
      plannerBuilder()
        .build()

    for ((comparison, comparisonExpression) <- comparisons) {
      withClue(comparison) {
        for (direction <- Seq(("-", "->"), ("<-", "-"), ("-", "-"))) {
          val query =
            s"""MATCH ()${direction._1}[r:ACTS_IN]${direction._2}()
               |  SEARCH r IN (
               |    VECTOR INDEX actsInScript
               |    FOR $$embedding
               |    WHERE r.workingDays $comparison
               |    LIMIT 5
               |  )
               |RETURN r""".stripMargin

          withClue(s"Testing direction: ${direction._1}[r:CONTRIBUTED]${direction._2}") {
            val properties = Seq("script", "workingDays")
            planner.plan(CypherVersion.Cypher25, query) should equal(
              planner.planBuilder()
                .produceResults("r")
                .relationshipVectorIndexSearch(
                  pattern = s"()${direction._1}[r]${direction._2}()",
                  typeNames = Seq("ACTS_IN"),
                  properties = properties,
                  indexName = "actsInScript",
                  vector = "$embedding",
                  limit = "5",
                  propertyFilter = Some(comparisonExpression)
                )
                .build()
            )
          }
        }
      }
    }
  }

  test("don't solve predicates already inlined in SEARCH") {
    val planner = plannerBuilder().build()
    val query = """MATCH (movie:Movie)
                  |WHERE
                  |  movie.releaseYear > 2000 AND
                  |  movie.releaseYear < 2010 AND
                  |  movie.imdbRating > 8
                  |SEARCH movie IN (
                  |  VECTOR INDEX moviePlots
                  |  FOR $embedding
                  |  WHERE
                  |    movie.releaseYear < 2010 AND
                  |    movie.imdbRating > 8
                  |  LIMIT 10
                  |)
                  |RETURN movie.plot""".stripMargin
    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.plot AS `movie.plot`")
      .filter("movie.releaseYear > 2000")
      .nodeVectorIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = Seq("plot", "imdbRating", "releaseYear"),
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        propertyFilter = Some(
          QueryExpressionConstructionTestSupport.composite(
            rangeExpression(gt(literalInt(8))),
            rangeExpression(lt(literalInt(2010)))
          )
        )
      )
      .build()
  }

  test("should apply selectivities of inlined SEARCH predicates to vector search cardinality") {
    val planner = plannerBuilder().build()

    case class TestPredicate(
      queryText: String,
      queryTextWithCaching: String,
      searchQuery: QueryExpression[Expression],
      selectivity: Selectivity
    )

    val imdbRatingEquals8 = TestPredicate(
      "movie.imdbRating = 8",
      "movie.imdbRating = 8",
      QueryExpressionConstructionTestSupport.single(literalInt(8)),
      PlannerDefaults.DEFAULT_EQUALITY_SELECTIVITY * PlannerDefaults.DEFAULT_PROPERTY_SELECTIVITY
    )

    val releaseYearLessThan2010 = TestPredicate(
      "movie.releaseYear < 2010",
      "movie.releaseYear < 2010",
      rangeExpression(lt(literalInt(2010))),
      PlannerDefaults.DEFAULT_RANGE_SELECTIVITY
    )

    for {
      imdbRatingPred <- Seq(None, Some(imdbRatingEquals8))
      releaseYearPred <- Seq(None, Some(releaseYearLessThan2010))
      if imdbRatingPred.isDefined || releaseYearPred.isDefined
      inlinePredicates <- Seq(true, false)
    } withClue(
      s"\nimdbRatingPred: $imdbRatingPred\nreleaseYearPred: $releaseYearPred\ninlinePredicates: $inlinePredicates\n"
    ) {

      val queryExpression: String = Seq(imdbRatingPred, releaseYearPred).flatten.map(_.queryText).mkString(" AND ")
      val planExpressions: Seq[String] = Seq(imdbRatingPred, releaseYearPred).flatten.map(_.queryTextWithCaching)

      val whereClause = "WHERE " + queryExpression

      val searchFilter = composite(
        imdbRatingPred.map(_.searchQuery).getOrElse(AllQueryExpression),
        releaseYearPred.map(_.searchQuery).getOrElse(AllQueryExpression)
      )

      val limit = 10
      val query =
        s"""MATCH (movie:Movie)
           |${if (inlinePredicates) "" else whereClause}
           |SEARCH movie IN (
           |  VECTOR INDEX moviePlots
           |  FOR $$embedding
           |  ${if (inlinePredicates) whereClause else ""}
           |  LIMIT $limit
           |)
           |RETURN movie.plot AS result""".stripMargin

      val expectedCardinality =
        limit *
          Seq(imdbRatingPred, releaseYearPred)
            .flatten
            .map(_.selectivity.factor)
            .product

      val expectedPlanBuilder = planner.subPlanBuilder()
        .produceResults("result").withCardinality(expectedCardinality)
        .projection("movie.plot AS result").withCardinality(expectedCardinality)
        .planIf(!inlinePredicates)(_.filter(planExpressions: _*).withCardinality(expectedCardinality))
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = Seq("plot", "imdbRating", "releaseYear"),
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          propertyFilter = Option.when(inlinePredicates)(searchFilter)
        ).withCardinality(if (inlinePredicates) expectedCardinality else limit)

      val actualPlanState = planner.planState(CypherVersion.Cypher25, query)

      withClue(s"query:\n$query\nexpectedCardinality: $expectedCardinality\n") {
        actualPlanState should haveSamePlanAndCardinalitiesAsBuilder(expectedPlanBuilder)
      }
    }
  }

  test("plan relationship vector index search with a previously bound relationship and a hidden type selection") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH ()-[r:CONTRIBUTED]->()
        |WITH r
        |MATCH ()-[r:CONTRIBUTED]->()
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN r.plot""".stripMargin

    val planState = planner.planState(CypherVersion.Cypher25, query)

    planState.logicalPlan shouldEqual
      planner.planBuilder()
        .produceResults("`r.plot`")
        .projection("r.plot AS `r.plot`")
        .filter("`  r@0` = r", "r:CONTRIBUTED")
        .apply()
        .|.relationshipVectorIndexSearch(
          pattern = "()-[`  r@0`]->()",
          typeNames = Seq("ACTS_IN"),
          properties = actsInScriptProperties,
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set("  UNNAMED0", "  UNNAMED1", "r"),
          getValueFromIndex = Map("script" -> DoNotGetValue, "workingDays" -> DoNotGetValue)
        )
        .relationshipTypeScan("(`  UNNAMED0`)-[r:CONTRIBUTED]->(`  UNNAMED1`)")
        .build()

    val selections = planState.logicalPlan.folder.findAllByClass[Selection]
    selections should not be empty
    selections.foreach { s =>
      withClue(s"labelAndRelTypeInfos should be defined for Selection with id ${s.id}:") {
        planState.planningAttributes.labelAndRelTypeInfos.isDefinedAt(s.id) shouldBe true
      }
    }
  }
}

abstract class VectorSearchWithComplexPatternPlanningIntegrationTestBase
    extends VectorSearchPlanningIntegrationTestBase {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .addSemanticFeature(VectorSearchWithComplexPattern)

  extension (str: String) {

    def at(pos: Int): String = {
      val name = AnonymousVariableNameGenerator.includeName(str, AnonymousVariableNameGenerator.anonymousVarName(pos))
      s"`$name`"
    }

    def unbacktick: String = str.stripPrefix("`").stripSuffix("`")

    def toVar: Variable = varFor(str)
  }

  test("single-hop pattern") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (director:Person)-[:DIRECTED]->(movie:Movie)
        |  WHERE director.born > 1950
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN director.name AS name, movie.title AS title""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val movie2 = "movie".at(2)
    val candidateIds3 = "candidateIds".at(3)
    val groups4 = "groups".at(4)
    val group5 = "group".at(5)
    val rows6 = "rows".at(6)
    val record7 = "record".at(7)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("director.name AS name", "movie.title AS title")
      .projection(s"$record7.$director0 AS director", s"$record7.$movie1 AS movie")
      .unwind(s"$group5.$rows6 AS $record7")
      .apply()
      .|.valueHashJoin(s"$group5.$movie1 = $movie2")
      .|.|.nodeVectorIndexSearch(
        node = movie2.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds3.unbacktick.toVar),
        argumentIds = Set(candidateIds3.unbacktick, groups4.unbacktick)
      )
      .|.unwind(s"$groups4 AS $group5")
      .|.argument(candidateIds3.unbacktick, groups4.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds3.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups4.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows6.unbacktick -> rows6.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(
          rows6.unbacktick -> collect(mapOf(
            director0.unbacktick -> director0.unbacktick.toVar,
            movie1.unbacktick -> movie1.unbacktick.toVar
          ))
        )
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($director0)-[:DIRECTED]->($movie1)")
      .filter(s"$director0.born > 1950")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("every retained variable is buffered and restored, including relationships") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (director:Person)-[rel:DIRECTED]->(movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN director, rel, movie""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val rel2 = "inner_rel".at(2)
    val movie3 = "movie".at(3)
    val candidateIds4 = "candidateIds".at(4)
    val groups5 = "groups".at(5)
    val group6 = "group".at(6)
    val rows7 = "rows".at(7)
    val record8 = "record".at(8)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection(s"$record8.$director0 AS director", s"$record8.$movie1 AS movie", s"$record8.$rel2 AS rel")
      .unwind(s"$group6.$rows7 AS $record8")
      .apply()
      .|.valueHashJoin(s"$group6.$movie1 = $movie3")
      .|.|.nodeVectorIndexSearch(
        node = movie3.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds4.unbacktick.toVar),
        argumentIds = Set(candidateIds4.unbacktick, groups5.unbacktick)
      )
      .|.unwind(s"$groups5 AS $group6")
      .|.argument(candidateIds4.unbacktick, groups5.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds4.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups5.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows7.unbacktick -> rows7.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(
          rows7.unbacktick -> collect(mapOf(
            director0.unbacktick -> director0.unbacktick.toVar,
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rel2.unbacktick -> rel2.unbacktick.toVar
          ))
        )
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($director0)-[$rel2:DIRECTED]->($movie1)")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("variables that are not retained are not buffered, but multiplicity is preserved") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (director:Person)-[:DIRECTED]->(movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie.title AS title""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val movie2 = "movie".at(2)
    val candidateIds3 = "candidateIds".at(3)
    val groups4 = "groups".at(4)
    val group5 = "group".at(5)
    val rows6 = "rows".at(6)
    val record7 = "record".at(7)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.title AS title")
      .projection(s"$record7.$director0 AS director", s"$record7.$movie1 AS movie")
      .unwind(s"$group5.$rows6 AS $record7")
      .apply()
      .|.valueHashJoin(s"$group5.$movie1 = $movie2")
      .|.|.nodeVectorIndexSearch(
        node = movie2.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds3.unbacktick.toVar),
        argumentIds = Set(candidateIds3.unbacktick, groups4.unbacktick)
      )
      .|.unwind(s"$groups4 AS $group5")
      .|.argument(candidateIds3.unbacktick, groups4.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds3.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups4.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows6.unbacktick -> rows6.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(
          rows6.unbacktick -> collect(mapOf(
            director0.unbacktick -> director0.unbacktick.toVar,
            movie1.unbacktick -> movie1.unbacktick.toVar
          ))
        )
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($director0)-[:DIRECTED]->($movie1)")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("multi-hop pattern with two retained variables") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (actor:Person)-[:ACTS_IN]->(movie:Movie)<-[:DIRECTED]-(director:Person)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN actor.name AS actor, director.name AS director, movie.title AS title""".stripMargin

    val actor0 = "actor".at(0)
    val director1 = "director".at(1)
    val actor2 = "actor".at(2)
    val director3 = "director".at(3)
    val innerActor4 = "inner_actor".at(4)
    val innerDirector5 = "inner_director".at(5)
    val innerMovie6 = "inner_movie".at(6)
    val movie7 = "movie".at(7)
    val candidateIds8 = "candidateIds".at(8)
    val groups9 = "groups".at(9)
    val group10 = "group".at(10)
    val rows11 = "rows".at(11)
    val record12 = "record".at(12)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection(s"$actor0.name AS $actor2", s"$director1.name AS $director3", "movie.title AS title")
      .projection(
        s"$record12.$innerActor4 AS $actor0",
        s"$record12.$innerDirector5 AS $director1",
        s"$record12.$innerMovie6 AS movie"
      )
      .unwind(s"$group10.$rows11 AS $record12")
      .apply()
      .|.valueHashJoin(s"$group10.$innerMovie6 = $movie7")
      .|.|.nodeVectorIndexSearch(
        node = movie7.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds8.unbacktick.toVar),
        argumentIds = Set(candidateIds8.unbacktick, groups9.unbacktick)
      )
      .|.unwind(s"$groups9 AS $group10")
      .|.argument(candidateIds8.unbacktick, groups9.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds8.unbacktick -> compileEntityFilter(id(innerMovie6.unbacktick.toVar), "moviePlots"),
          groups9.unbacktick -> collect(mapOf(
            innerMovie6.unbacktick -> innerMovie6.unbacktick.toVar,
            rows11.unbacktick -> rows11.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(innerMovie6.unbacktick -> innerMovie6.unbacktick.toVar),
        Map(
          rows11.unbacktick -> collect(mapOf(
            innerActor4.unbacktick -> innerActor4.unbacktick.toVar,
            innerDirector5.unbacktick -> innerDirector5.unbacktick.toVar,
            innerMovie6.unbacktick -> innerMovie6.unbacktick.toVar
          ))
        )
      )
      .filter(s"$innerActor4:Person")
      .expandAll(s"($innerMovie6)<-[:ACTS_IN]-($innerActor4)")
      .filter(s"$innerMovie6:Movie")
      .expandAll(s"($innerDirector5)-[:DIRECTED]->($innerMovie6)")
      .nodeByLabelScan(innerDirector5, "Person")
      .build()
  }

  test("in-index property filter from SEARCH WHERE is combined with the entity filter") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (director:Person)-[:DIRECTED]->(movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    WHERE movie.imdbRating > 8
        |    LIMIT 10
        |  )
        |RETURN director.name AS name, movie.title AS title""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val movie2 = "movie".at(2)
    val candidateIds3 = "candidateIds".at(3)
    val groups4 = "groups".at(4)
    val group5 = "group".at(5)
    val rows6 = "rows".at(6)
    val record7 = "record".at(7)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("director.name AS name", "movie.title AS title")
      .projection(s"$record7.$director0 AS director", s"$record7.$movie1 AS movie")
      .unwind(s"$group5.$rows6 AS $record7")
      .apply()
      .|.valueHashJoin(s"$group5.$movie1 = $movie2")
      .|.|.nodeVectorIndexSearch(
        node = movie2.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds3.unbacktick.toVar),
        propertyFilter = Some(composite(rangeExpression(gt(literalInt(8))), AllQueryExpression)),
        argumentIds = Set(candidateIds3.unbacktick, groups4.unbacktick)
      )
      .|.unwind(s"$groups4 AS $group5")
      .|.argument(candidateIds3.unbacktick, groups4.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds3.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups4.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows6.unbacktick -> rows6.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(
          rows6.unbacktick -> collect(mapOf(
            director0.unbacktick -> director0.unbacktick.toVar,
            movie1.unbacktick -> movie1.unbacktick.toVar
          ))
        )
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($director0)-[:DIRECTED]->($movie1)")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("pattern WHERE predicate on an indexed property is solved in the pattern") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (director:Person)-[:DIRECTED]->(movie:Movie)
        |  WHERE movie.releaseYear = 1989
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN director.name AS name, movie.title AS title""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val movie2 = "movie".at(2)
    val candidateIds3 = "candidateIds".at(3)
    val groups4 = "groups".at(4)
    val group5 = "group".at(5)
    val rows6 = "rows".at(6)
    val record7 = "record".at(7)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("director.name AS name", "movie.title AS title")
      .projection(s"$record7.$director0 AS director", s"$record7.$movie1 AS movie")
      .unwind(s"$group5.$rows6 AS $record7")
      .apply()
      .|.valueHashJoin(s"$group5.$movie1 = $movie2")
      .|.|.nodeVectorIndexSearch(
        node = movie2.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds3.unbacktick.toVar),
        argumentIds = Set(candidateIds3.unbacktick, groups4.unbacktick)
      )
      .|.unwind(s"$groups4 AS $group5")
      .|.argument(candidateIds3.unbacktick, groups4.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds3.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups4.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows6.unbacktick -> rows6.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(
          rows6.unbacktick -> collect(mapOf(
            director0.unbacktick -> director0.unbacktick.toVar,
            movie1.unbacktick -> movie1.unbacktick.toVar
          ))
        )
      )
      .filter(s"$movie1.releaseYear = 1989", s"$movie1:Movie")
      .expandAll(s"($director0)-[:DIRECTED]->($movie1)")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("disconnected components are connected via CartesianProduct below the aggregations") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (person:Person), (director:Person)-[:DIRECTED]->(movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN person.name AS person, director.name AS director, movie.title AS title""".stripMargin

    val person0 = "person".at(0)
    val director1 = "director".at(1)
    val person2 = "person".at(2)
    val director3 = "director".at(3)
    val innerDirector4 = "inner_director".at(4)
    val innerPerson5 = "inner_person".at(5)
    val innerMovie6 = "inner_movie".at(6)
    val movie7 = "movie".at(7)
    val candidateIds8 = "candidateIds".at(8)
    val groups9 = "groups".at(9)
    val group10 = "group".at(10)
    val rows11 = "rows".at(11)
    val record12 = "record".at(12)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection(s"$person0.name AS $person2", s"$director1.name AS $director3", "movie.title AS title")
      .projection(
        s"$record12.$innerDirector4 AS $director1",
        s"$record12.$innerPerson5 AS $person0",
        s"$record12.$innerMovie6 AS movie"
      )
      .unwind(s"$group10.$rows11 AS $record12")
      .apply()
      .|.valueHashJoin(s"$group10.$innerMovie6 = $movie7")
      .|.|.nodeVectorIndexSearch(
        node = movie7.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds8.unbacktick.toVar),
        argumentIds = Set(candidateIds8.unbacktick, groups9.unbacktick)
      )
      .|.unwind(s"$groups9 AS $group10")
      .|.argument(candidateIds8.unbacktick, groups9.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds8.unbacktick -> compileEntityFilter(id(innerMovie6.unbacktick.toVar), "moviePlots"),
          groups9.unbacktick -> collect(mapOf(
            innerMovie6.unbacktick -> innerMovie6.unbacktick.toVar,
            rows11.unbacktick -> rows11.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(innerMovie6.unbacktick -> innerMovie6.unbacktick.toVar),
        Map(
          rows11.unbacktick -> collect(mapOf(
            innerDirector4.unbacktick -> innerDirector4.unbacktick.toVar,
            innerMovie6.unbacktick -> innerMovie6.unbacktick.toVar,
            innerPerson5.unbacktick -> innerPerson5.unbacktick.toVar
          ))
        )
      )
      .cartesianProduct()
      .|.filter(s"$innerMovie6:Movie")
      .|.expandAll(s"($innerDirector4)-[:DIRECTED]->($innerMovie6)")
      .|.nodeByLabelScan(innerDirector4, "Person")
      .nodeByLabelScan(innerPerson5, "Person")
      .build()
  }

  test("var-length relationship pattern") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (movie:Movie)-[r*2..4]->(endNode)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN movie.plot AS plot""".stripMargin

    val endNode0 = "inner_endNode".at(0)
    val movie1 = "inner_movie".at(1)
    val r2 = "inner_r".at(2)
    val movie3 = "movie".at(3)
    val candidateIds4 = "candidateIds".at(4)
    val groups5 = "groups".at(5)
    val group6 = "group".at(6)
    val rows7 = "rows".at(7)
    val record8 = "record".at(8)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.plot AS plot")
      .projection(
        s"$record8.$endNode0 AS endNode",
        s"$record8.$movie1 AS movie",
        s"$record8.$r2 AS r"
      )
      .unwind(s"$group6.$rows7 AS $record8")
      .apply()
      .|.valueHashJoin(s"$group6.$movie1 = $movie3")
      .|.|.nodeVectorIndexSearch(
        node = movie3.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds4.unbacktick.toVar),
        argumentIds = Set(candidateIds4.unbacktick, groups5.unbacktick)
      )
      .|.unwind(s"$groups5 AS $group6")
      .|.argument(candidateIds4.unbacktick, groups5.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds4.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups5.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows7.unbacktick -> rows7.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(
          rows7.unbacktick -> collect(mapOf(
            endNode0.unbacktick -> endNode0.unbacktick.toVar,
            movie1.unbacktick -> movie1.unbacktick.toVar,
            r2.unbacktick -> r2.unbacktick.toVar
          ))
        )
      )
      .expand(s"($movie1)-[$r2*2..4]->($endNode0)")
      .nodeByLabelScan(movie1, "Movie")
      .build()
  }

  test("QPP with a singleton search variable") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (movie:Movie) ((x:Movie)<-[p:DIRECTED]-(dir)-[q:DIRECTED]->(y:Movie)){1, 4} (otherMovie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |WHERE movie.year = otherMovie.year
        |RETURN movie.plot AS plot""".stripMargin

    val x0 = "x".at(0)
    val p1 = "p".at(1)
    val dir2 = "dir".at(2)
    val q3 = "q".at(3)
    val y4 = "y".at(4)
    val pGroup5 = "p".at(5)
    val xGroup6 = "x".at(6)
    val dirGroup7 = "dir".at(7)
    val qGroup8 = "q".at(8)
    val yGroup9 = "y".at(9)

    val dirGroupInner = "inner_dir".at(10)
    val pGroupInner = "inner_p".at(11)
    val qGroupInner = "inner_q".at(12)
    val xGroupInner = "inner_x".at(13)
    val yGroupInner = "inner_y".at(14)
    val movie15 = "inner_movie".at(15)
    val otherMovie16 = "inner_otherMovie".at(16)
    val movie17 = "movie".at(17)
    val candidateIds18 = "candidateIds".at(18)
    val groups19 = "groups".at(19)
    val group20 = "group".at(20)
    val rows21 = "rows".at(21)
    val record22 = "record".at(22)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    val trailParameters = TrailParameters(
      min = 1,
      max = UpperBound.Limited(4),
      start = otherMovie16.unbacktick,
      end = movie15.unbacktick,
      innerStart = y4.unbacktick,
      innerEnd = x0.unbacktick,
      groupNodes = Set(
        x0.unbacktick -> xGroupInner.unbacktick,
        dir2.unbacktick -> dirGroupInner.unbacktick,
        y4.unbacktick -> yGroupInner.unbacktick
      ),
      groupRelationships = Set(
        p1.unbacktick -> pGroupInner.unbacktick,
        q3.unbacktick -> qGroupInner.unbacktick
      ),
      innerRelationships = Set(p1.unbacktick, q3.unbacktick),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set()
    )
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.plot AS plot")
      .projection(
        s"$record22.$dirGroupInner AS $dirGroup7",
        s"$record22.$xGroupInner AS $xGroup6",
        s"$record22.$pGroupInner AS $pGroup5",
        s"$record22.$otherMovie16 AS otherMovie",
        s"$record22.$qGroupInner AS $qGroup8",
        s"$record22.$yGroupInner AS $yGroup9",
        s"$record22.$movie15 AS movie"
      )
      .unwind(s"$group20.$rows21 AS $record22")
      .apply()
      .|.valueHashJoin(s"$group20.$movie15 = $movie17")
      .|.|.nodeVectorIndexSearch(
        node = movie17.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds18.unbacktick.toVar),
        argumentIds = Set(candidateIds18.unbacktick, groups19.unbacktick)
      )
      .|.unwind(s"$groups19 AS $group20")
      .|.argument(candidateIds18.unbacktick, groups19.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds18.unbacktick -> compileEntityFilter(id(movie15.unbacktick.toVar), "moviePlots"),
          groups19.unbacktick -> collect(mapOf(
            movie15.unbacktick -> movie15.unbacktick.toVar,
            rows21.unbacktick -> rows21.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie15.unbacktick -> movie15.unbacktick.toVar),
        Map(rows21.unbacktick -> collect(mapOf(
          dirGroupInner.unbacktick -> dirGroupInner.unbacktick.toVar,
          movie15.unbacktick -> movie15.unbacktick.toVar,
          otherMovie16.unbacktick -> otherMovie16.unbacktick.toVar,
          pGroupInner.unbacktick -> pGroupInner.unbacktick.toVar,
          qGroupInner.unbacktick -> qGroupInner.unbacktick.toVar,
          xGroupInner.unbacktick -> xGroupInner.unbacktick.toVar,
          yGroupInner.unbacktick -> yGroupInner.unbacktick.toVar
        )))
      )
      .filter(s"$movie15.year = $otherMovie16.year", s"$movie15:Movie")
      .repeatTrail(trailParameters)
      .|.filter(s"NOT $q3 = $p1", isRepeatTrailUnique(p1.unbacktick), s"$x0:Movie")
      .|.expandAll(s"($dir2)-[$p1:DIRECTED]->($x0)")
      .|.filter(isRepeatTrailUnique(q3.unbacktick))
      .|.expandAll(s"($y4)<-[$q3:DIRECTED]-($dir2)")
      .|.filter(s"$y4:Movie")
      .|.argument(y4.unbacktick)
      .nodeByLabelScan(otherMovie16, "Movie")
      .build()
  }

  test("OPTIONAL MATCH with a complex pattern") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """OPTIONAL MATCH (director:Person)-[:DIRECTED]->(movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN director.name AS name, movie.title AS title""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val movie2 = "movie".at(2)
    val candidateIds3 = "candidateIds".at(3)
    val groups4 = "groups".at(4)
    val group5 = "group".at(5)
    val rows6 = "rows".at(6)
    val record7 = "record".at(7)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("director.name AS name", "movie.title AS title")
      .optional()
      .projection(s"$record7.$director0 AS director", s"$record7.$movie1 AS movie")
      .unwind(s"$group5.$rows6 AS $record7")
      .apply()
      .|.valueHashJoin(s"$group5.$movie1 = $movie2")
      .|.|.nodeVectorIndexSearch(
        node = movie2.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds3.unbacktick.toVar),
        argumentIds = Set(candidateIds3.unbacktick, groups4.unbacktick)
      )
      .|.unwind(s"$groups4 AS $group5")
      .|.argument(candidateIds3.unbacktick, groups4.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds3.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups4.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows6.unbacktick -> rows6.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(rows6.unbacktick -> collect(mapOf(
          director0.unbacktick -> director0.unbacktick.toVar,
          movie1.unbacktick -> movie1.unbacktick.toVar
        )))
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($director0)-[:DIRECTED]->($movie1)")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("embedding depending on a pattern symbol groups the search by that symbol") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    // A correlated query vector groups candidates by its dependencies, applying LIMIT per distinct vector.
    val query =
      """MATCH (director:Person)-[:DIRECTED]->(movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR director.embedding
        |    LIMIT 10
        |  )
        |RETURN director.name AS name, movie.title AS title""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val movie2 = "movie".at(2)
    val candidateIds3 = "candidateIds".at(3)
    val groups4 = "groups".at(4)
    val group5 = "group".at(5)
    val rows6 = "rows".at(6)
    val record7 = "record".at(7)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("director.name AS name", "movie.title AS title")
      .projection(s"$record7.$director0 AS director", s"$record7.$movie1 AS movie")
      .unwind(s"$group5.$rows6 AS $record7")
      .apply()
      .|.valueHashJoin(s"$group5.$movie1 = $movie2")
      .|.|.nodeVectorIndexSearch(
        node = movie2.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = prop(director0.unbacktick, "embedding"),
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds3.unbacktick.toVar),
        argumentIds = Set(director0.unbacktick, candidateIds3.unbacktick, groups4.unbacktick)
      )
      .|.unwind(s"$groups4 AS $group5")
      .|.argument(director0.unbacktick, candidateIds3.unbacktick, groups4.unbacktick)
      .aggregation(
        Map(director0.unbacktick -> director0.unbacktick.toVar),
        Map(
          candidateIds3.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups4.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows6.unbacktick -> rows6.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(director0.unbacktick -> director0.unbacktick.toVar, movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(rows6.unbacktick -> collect(mapOf(
          director0.unbacktick -> director0.unbacktick.toVar,
          movie1.unbacktick -> movie1.unbacktick.toVar
        )))
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($director0)-[:DIRECTED]->($movie1)")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("in-index property filter depending on a pattern symbol groups the search by that symbol") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    // A correlated in-index predicate groups candidates by its dependencies, running one search per distinct value.
    val query =
      """MATCH (director:Person)-[:DIRECTED]->(movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    WHERE movie.releaseYear < director.birthYear
        |    LIMIT 10
        |  )
        |RETURN director.name AS name, movie.title AS title""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val movie2 = "movie".at(2)
    val candidateIds3 = "candidateIds".at(3)
    val groups4 = "groups".at(4)
    val group5 = "group".at(5)
    val rows6 = "rows".at(6)
    val record7 = "record".at(7)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("director.name AS name", "movie.title AS title")
      .projection(s"$record7.$director0 AS director", s"$record7.$movie1 AS movie")
      .unwind(s"$group5.$rows6 AS $record7")
      .apply()
      .|.valueHashJoin(s"$group5.$movie1 = $movie2")
      .|.|.nodeVectorIndexSearch(
        node = movie2.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds3.unbacktick.toVar),
        propertyFilter =
          Some(composite(AllQueryExpression, rangeExpression(lt(prop(director0.unbacktick, "birthYear"))))),
        argumentIds = Set(director0.unbacktick, candidateIds3.unbacktick, groups4.unbacktick)
      )
      .|.unwind(s"$groups4 AS $group5")
      .|.argument(director0.unbacktick, candidateIds3.unbacktick, groups4.unbacktick)
      .aggregation(
        Map(director0.unbacktick -> director0.unbacktick.toVar),
        Map(
          candidateIds3.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups4.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows6.unbacktick -> rows6.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(director0.unbacktick -> director0.unbacktick.toVar, movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(rows6.unbacktick -> collect(mapOf(
          director0.unbacktick -> director0.unbacktick.toVar,
          movie1.unbacktick -> movie1.unbacktick.toVar
        )))
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($director0)-[:DIRECTED]->($movie1)")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("score variable is bound by the search leaf and carried through the replay") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (director:Person)-[:DIRECTED]->(movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  ) SCORE AS similarity
        |RETURN director.name AS name, movie.title AS title, similarity""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val movie2 = "movie".at(2)
    val candidateIds3 = "candidateIds".at(3)
    val groups4 = "groups".at(4)
    val group5 = "group".at(5)
    val rows6 = "rows".at(6)
    val record7 = "record".at(7)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("director.name AS name", "movie.title AS title")
      .projection(s"$record7.$director0 AS director", s"$record7.$movie1 AS movie")
      .unwind(s"$group5.$rows6 AS $record7")
      .apply()
      .|.valueHashJoin(s"$group5.$movie1 = $movie2")
      .|.|.nodeVectorIndexSearch(
        node = movie2.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        score = "similarity",
        entityFilter = preparedEntityFilter(candidateIds3.unbacktick.toVar),
        argumentIds = Set(candidateIds3.unbacktick, groups4.unbacktick)
      )
      .|.unwind(s"$groups4 AS $group5")
      .|.argument(candidateIds3.unbacktick, groups4.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds3.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups4.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows6.unbacktick -> rows6.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(rows6.unbacktick -> collect(mapOf(
          director0.unbacktick -> director0.unbacktick.toVar,
          movie1.unbacktick -> movie1.unbacktick.toVar
        )))
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($director0)-[:DIRECTED]->($movie1)")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("predicate combining score and a pattern variable is applied as a post-filter") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (director:Person)-[rel:DIRECTED]->(movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  ) SCORE AS score
        |WHERE score > rel.weight
        |RETURN director.name AS name, movie.title AS title""".stripMargin

    val director0 = "inner_director".at(0)
    val movie1 = "inner_movie".at(1)
    val rel2 = "inner_rel".at(2)
    val movie3 = "movie".at(3)
    val candidateIds4 = "candidateIds".at(4)
    val groups5 = "groups".at(5)
    val group6 = "group".at(6)
    val rows7 = "rows".at(7)
    val record8 = "record".at(8)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("director.name AS name", "movie.title AS title")
      .filter("rel.weight < score")
      .projection(s"$record8.$director0 AS director", s"$record8.$movie1 AS movie", s"$record8.$rel2 AS rel")
      .unwind(s"$group6.$rows7 AS $record8")
      .apply()
      .|.valueHashJoin(s"$group6.$movie1 = $movie3")
      .|.|.nodeVectorIndexSearch(
        node = movie3.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        score = "score",
        entityFilter = preparedEntityFilter(candidateIds4.unbacktick.toVar),
        argumentIds = Set(candidateIds4.unbacktick, groups5.unbacktick)
      )
      .|.unwind(s"$groups5 AS $group6")
      .|.argument(candidateIds4.unbacktick, groups5.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds4.unbacktick -> compileEntityFilter(id(movie1.unbacktick.toVar), "moviePlots"),
          groups5.unbacktick -> collect(mapOf(
            movie1.unbacktick -> movie1.unbacktick.toVar,
            rows7.unbacktick -> rows7.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(movie1.unbacktick -> movie1.unbacktick.toVar),
        Map(rows7.unbacktick -> collect(mapOf(
          director0.unbacktick -> director0.unbacktick.toVar,
          movie1.unbacktick -> movie1.unbacktick.toVar,
          rel2.unbacktick -> rel2.unbacktick.toVar
        )))
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($director0)-[$rel2:DIRECTED]->($movie1)")
      .nodeByLabelScan(director0, "Person")
      .build()
  }

  test("relationship search with a complex pattern") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (actor:Person)-[r:ACTS_IN]->(movie:Movie)
        |  WHERE actor.born > 1950
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN actor.name AS name, movie.title AS title""".stripMargin

    val actor0 = "inner_actor".at(0)
    val movie1 = "inner_movie".at(1)
    val r2 = "inner_r".at(2)
    val r3 = "r".at(3)
    val candidateIds4 = "candidateIds".at(4)
    val groups5 = "groups".at(5)
    val group6 = "group".at(6)
    val rows7 = "rows".at(7)
    val record8 = "record".at(8)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("actor.name AS name", "movie.title AS title")
      .projection(s"$record8.$actor0 AS actor", s"$record8.$movie1 AS movie", s"$record8.$r2 AS r")
      .unwind(s"$group6.$rows7 AS $record8")
      .apply()
      .|.valueHashJoin(s"$group6.$r2 = $r3")
      .|.|.relationshipVectorIndexSearch(
        pattern = s"()-[$r3]->()",
        typeNames = Seq("ACTS_IN"),
        properties = actsInScriptProperties,
        indexName = "actsInScript",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds4.unbacktick.toVar),
        argumentIds = Set(candidateIds4.unbacktick, groups5.unbacktick)
      )
      .|.unwind(s"$groups5 AS $group6")
      .|.argument(candidateIds4.unbacktick, groups5.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds4.unbacktick -> compileEntityFilter(id(r2.unbacktick.toVar), "actsInScript"),
          groups5.unbacktick -> collect(mapOf(
            r2.unbacktick -> r2.unbacktick.toVar,
            rows7.unbacktick -> rows7.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(r2.unbacktick -> r2.unbacktick.toVar),
        Map(rows7.unbacktick -> collect(mapOf(
          actor0.unbacktick -> actor0.unbacktick.toVar,
          movie1.unbacktick -> movie1.unbacktick.toVar,
          r2.unbacktick -> r2.unbacktick.toVar
        )))
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($actor0)-[$r2:ACTS_IN]->($movie1)")
      .filter(s"$actor0.born > 1950")
      .nodeByLabelScan(actor0, "Person")
      .build()
  }

  test("relationship search combining entity filter and in-index property filter") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (actor:Person)-[r:ACTS_IN]->(movie:Movie)
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    WHERE r.workingDays > 5
        |    LIMIT 10
        |  )
        |RETURN actor.name AS name, movie.title AS title""".stripMargin

    val actor0 = "inner_actor".at(0)
    val movie1 = "inner_movie".at(1)
    val r2 = "inner_r".at(2)
    val r3 = "r".at(3)
    val candidateIds4 = "candidateIds".at(4)
    val groups5 = "groups".at(5)
    val group6 = "group".at(6)
    val rows7 = "rows".at(7)
    val record8 = "record".at(8)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("actor.name AS name", "movie.title AS title")
      .projection(s"$record8.$actor0 AS actor", s"$record8.$movie1 AS movie", s"$record8.$r2 AS r")
      .unwind(s"$group6.$rows7 AS $record8")
      .apply()
      .|.valueHashJoin(s"$group6.$r2 = $r3")
      .|.|.relationshipVectorIndexSearch(
        pattern = s"()-[$r3]->()",
        typeNames = Seq("ACTS_IN"),
        properties = actsInScriptProperties,
        indexName = "actsInScript",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds4.unbacktick.toVar),
        propertyFilter = Some(rangeExpression(gt(literalInt(5)))),
        argumentIds = Set(candidateIds4.unbacktick, groups5.unbacktick)
      )
      .|.unwind(s"$groups5 AS $group6")
      .|.argument(candidateIds4.unbacktick, groups5.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds4.unbacktick -> compileEntityFilter(id(r2.unbacktick.toVar), "actsInScript"),
          groups5.unbacktick -> collect(mapOf(
            r2.unbacktick -> r2.unbacktick.toVar,
            rows7.unbacktick -> rows7.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(r2.unbacktick -> r2.unbacktick.toVar),
        Map(rows7.unbacktick -> collect(mapOf(
          actor0.unbacktick -> actor0.unbacktick.toVar,
          movie1.unbacktick -> movie1.unbacktick.toVar,
          r2.unbacktick -> r2.unbacktick.toVar
        )))
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($actor0)-[$r2:ACTS_IN]->($movie1)")
      .nodeByLabelScan(actor0, "Person")
      .build()
  }

  test("undirected relationship search with a complex pattern") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .setRelationshipCardinality("(:Movie)-[:ACTS_IN]->(:Person)", 50)
      .setRelationshipCardinality("()-[:ACTS_IN]->(:Person)", 50)
      .setRelationshipCardinality("(:Movie)-[:ACTS_IN]->()", 50)
      .build()

    val query =
      """MATCH (actor:Person)-[r:ACTS_IN]-(movie:Movie)
        |  SEARCH r IN (
        |    VECTOR INDEX actsInScript
        |    FOR $embedding
        |    LIMIT 10
        |  ) SCORE AS similarity
        |RETURN actor.name AS name, movie.title AS title""".stripMargin

    val actor0 = "inner_actor".at(0)
    val movie1 = "inner_movie".at(1)
    val r2 = "inner_r".at(2)
    val r3 = "r".at(3)
    val candidateIds4 = "candidateIds".at(4)
    val groups5 = "groups".at(5)
    val group6 = "group".at(6)
    val rows7 = "rows".at(7)
    val record8 = "record".at(8)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("actor.name AS name", "movie.title AS title")
      .projection(s"$record8.$actor0 AS actor", s"$record8.$movie1 AS movie", s"$record8.$r2 AS r")
      .unwind(s"$group6.$rows7 AS $record8")
      .apply()
      .|.valueHashJoin(s"$group6.$r2 = $r3")
      .|.|.distinct(s"$r3 AS $r3", "similarity AS similarity")
      .|.|.relationshipVectorIndexSearch(
        pattern = s"()-[$r3]-()",
        typeNames = Seq("ACTS_IN"),
        properties = actsInScriptProperties,
        indexName = "actsInScript",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds4.unbacktick.toVar),
        score = "similarity",
        argumentIds = Set(candidateIds4.unbacktick, groups5.unbacktick)
      )
      .|.unwind(s"$groups5 AS $group6")
      .|.argument(candidateIds4.unbacktick, groups5.unbacktick)
      .aggregation(
        Map.empty,
        Map(
          candidateIds4.unbacktick -> compileEntityFilter(id(r2.unbacktick.toVar), "actsInScript"),
          groups5.unbacktick -> collect(mapOf(
            r2.unbacktick -> r2.unbacktick.toVar,
            rows7.unbacktick -> rows7.unbacktick.toVar
          ))
        )
      )
      .aggregation(
        Map(r2.unbacktick -> r2.unbacktick.toVar),
        Map(rows7.unbacktick -> collect(mapOf(
          actor0.unbacktick -> actor0.unbacktick.toVar,
          movie1.unbacktick -> movie1.unbacktick.toVar,
          r2.unbacktick -> r2.unbacktick.toVar
        )))
      )
      .filter(s"$movie1:Movie")
      .expandAll(s"($actor0)-[$r2:ACTS_IN]-($movie1)")
      .nodeByLabelScan(actor0, "Person")
      .build()
  }

  test("complex pattern referencing a previously bound pattern variable") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .setRelationshipCardinality("(:Actor)-[:ACTS_IN]->()", 50)
      .setRelationshipCardinality("(:Actor)-[:ACTS_IN]->(:Movie)", 50)
      .build()

    val query =
      """MATCH (actor:Actor)
        |MATCH (movie:Movie)<-[:ACTS_IN]-(actor)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR $embedding
        |    LIMIT 10
        |  )
        |RETURN actor.name AS name, movie.title AS title""".stripMargin

    val movie0 = "inner_movie".at(0)
    val movie1 = "movie".at(1)
    val candidateIds2 = "candidateIds".at(2)
    val groups3 = "groups".at(3)
    val group4 = "group".at(4)
    val rows5 = "rows".at(5)
    val record6 = "record".at(6)

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[actor.name] AS name", "movie.title AS title")
      .projection(s"$record6.$movie0 AS movie")
      .apply()
      .|.unwind(s"$group4.$rows5 AS $record6")
      .|.apply()
      .|.|.valueHashJoin(s"$group4.$movie0 = $movie1")
      .|.|.|.nodeVectorIndexSearch(
        node = movie1.unbacktick,
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        entityFilter = preparedEntityFilter(candidateIds2.unbacktick.toVar),
        argumentIds = Set(candidateIds2.unbacktick, groups3.unbacktick, "actor")
      )
      .|.|.unwind(s"$groups3 AS $group4")
      .|.|.argument(candidateIds2.unbacktick, groups3.unbacktick, "actor")
      .|.cacheProperties("cacheNFromStore[actor.name]")
      .|.aggregation(
        Map.empty,
        Map(
          candidateIds2.unbacktick -> compileEntityFilter(id(movie0.unbacktick.toVar), "moviePlots"),
          groups3.unbacktick -> collect(mapOf(
            movie0.unbacktick -> movie0.unbacktick.toVar,
            rows5.unbacktick -> rows5.unbacktick.toVar
          ))
        )
      )
      .|.aggregation(
        Map(movie0.unbacktick -> movie0.unbacktick.toVar),
        Map(rows5.unbacktick -> collect(mapOf(movie0.unbacktick -> movie0.unbacktick.toVar)))
      )
      .|.filter(s"$movie0:Movie")
      .|.expandAll(s"(actor)-[:ACTS_IN]->($movie0)")
      .|.argument("actor")
      .nodeByLabelScan("actor", "Actor")
      .build()
  }

  test("plan vector index search with OPTIONAL MATCH and DISTINCT score") {
    val planner = plannerBuilder().build()

    val query =
      """OPTIONAL MATCH (movie:Movie)
        |SEARCH movie IN (
        |  VECTOR INDEX moviePlots
        |  FOR $embedding
        |  LIMIT 10
        |) SCORE AS s
        |RETURN DISTINCT s""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .distinct("s AS s")
      .optional()
      .nodeVectorIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = Seq("plot", "imdbRating", "releaseYear"),
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        score = "s"
      )
      .build()
  }

}
