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
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.VectorSearch
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.VectorSingleStageFilteringEnabled
import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.VectorIndexSearchException

class VectorSearchPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanConstructionTestSupport
    with QueryExpressionConstructionTestSupport
    with LogicalPlanningAttributesTestSupport {

  private def movieLabelCardinality: Double = 80.0

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .addSemanticFeature(VectorSearch)
      .addSemanticFeature(VectorSingleStageFilteringEnabled)
      .setAllNodesCardinality(120)
      .setLabelCardinality("Movie", movieLabelCardinality)
      .setLabelCardinality("Actor", 40)
      .setLabelCardinality("Person", 10)
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

  private val moviePlotsProperties = Seq("plot", "imdbRating", "releaseYear")
  private val actsInScriptProperties = Seq("script", "workingDays")

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
        .projection("cacheN[movie.plot] AS `movie.plot`")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = { case "plot" => GetValue; case _ => DoNotGetValue }
        )
        .build()
  }

  test("should plan vector index operator with all labels in the correct order") {
    val planner =
      plannerBuilder()
        // This hopefully mixes up the order of label ids
        .setLabelCardinality("C", 5)
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
        .projection("cacheN[n.prop] AS `n.prop`")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
          properties = Seq("prop"),
          indexName = "lotsOfLabels",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = Map("prop" -> GetValue)
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
        .projection("cacheN[movie.info] AS `movie.info`")
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
          argumentIds = Set(),
          getValueFromIndex = Map("info" -> GetValue)
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
        .produceResults(column("movie", "cacheN[movie.plot]", "cacheN[movie.imdbRating]", "cacheN[movie.releaseYear]"))
        .expandAll("(movie)-[]->()")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = _ => GetValue
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
        .produceResults(column("movie", "cacheN[movie.plot]", "cacheN[movie.imdbRating]", "cacheN[movie.releaseYear]"))
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "embedding",
          limit = "10",
          argumentIds = Set("embedding"),
          getValueFromIndex = _ => GetValue
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
        .produceResults(column("movie", "cacheN[movie.plot]", "cacheN[movie.imdbRating]", "cacheN[movie.releaseYear]"))
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "anon_0",
          limit = "10",
          argumentIds = Set("anon_0"),
          getValueFromIndex = _ => GetValue
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
        .produceResults(column("movie", "cacheN[movie.plot]", "cacheN[movie.imdbRating]", "cacheN[movie.releaseYear]"))
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "$limit",
          getValueFromIndex = _ => GetValue
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
        .produceResults(
          column("movie", "cacheN[movie.plot]", "cacheN[movie.imdbRating]", "cacheN[movie.releaseYear]"),
          column("similarity")
        )
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          score = "similarity",
          argumentIds = Set(),
          getValueFromIndex = _ => GetValue
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
      .filter("similarity > 0.8")
      .nodeVectorIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = Seq("plot", "imdbRating", "releaseYear"),
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        score = "similarity",
        argumentIds = Set(),
        getValueFromIndex = Map("plot" -> GetValue, "imdbRating" -> DoNotGetValue, "releaseYear" -> DoNotGetValue)
      )
      .build()
  }

  // TODO: see Example SQ2 in CIP-224
  //  See PLAN-3087
  ignore("plan node vector index search, where the embedding refers to the binding variable") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
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
        .produceResults(column("movie", "cacheN[movie.plot]"))
        .apply()
        .|.filter("anon_0 = movie")
        .|.nodeVectorIndexSearch(
          node = "anon_0",
          labelNames = Seq("Movie"),
          properties = Seq("plot"),
          indexName = "moviePlots",
          vector = "movie.titleEmbedding",
          limit = "10",
          argumentIds = Set("movie"),
          getValueFromIndex = _ => GetValue
        )
        .filter("movie.title IN $titles")
        .nodeByLabelScan("movie", "Movie")
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
        .projection("cacheN[movie.plot] AS `movie.plot`")
        // movie.plot IS NOT NULL is solved by vector search implicitly so we don't need to check movie.plot2 = 'someValue'
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = { case "plot" => GetValue; case _ => DoNotGetValue }
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

    val caughtException = intercept[VectorIndexSearchException] {
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

    val caughtException = intercept[VectorIndexSearchException] {
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

    val caughtException = intercept[VectorIndexSearchException] {
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
        .produceResults(column("r", "cacheR[r.script]", "cacheR[r.workingDays]"))
        .relationshipVectorIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = Map("script" -> GetValue, "workingDays" -> GetValue)
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
        .produceResults(column("r", "cacheR[r.script]", "cacheR[r.workingDays]"))
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("ACTS_IN"),
          properties = Seq("script", "workingDays"),
          indexName = "actsInScript",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = Map("script" -> GetValue, "workingDays" -> GetValue)
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
        .projection("cacheR[r.plot] AS `r.plot`")
        .filter(hasTypes("r", "CONTRIBUTED"))
        .cacheProperties("cacheRFromStore[r.plot]")
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
        .projection("cacheR[r.plot] AS `r.plot`")
        .filter(andsReorderableAst(hasTypes("r", "ACTS_IN"), hasTypes("r", "CONTRIBUTED")))
        .cacheProperties("cacheRFromStore[r.plot]")
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
        .produceResults(
          column("movie", "cacheN[movie.plot]", "cacheN[movie.imdbRating]", "cacheN[movie.releaseYear]"),
          column("person")
        )
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "person.embedding",
          limit = "10",
          argumentIds = Set("person"),
          getValueFromIndex = _ => GetValue
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
        .projection("cacheN[movie.plot] AS `movie.plot`")
        .filter("similarity > 0.8")
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          score = "similarity",
          getValueFromIndex = { case "plot" => GetValue; case _ => DoNotGetValue }
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
        .produceResults(column("movie", "cacheN[movie.plot]", "cacheN[movie.imdbRating]", "cacheN[movie.releaseYear]"))
        .filter(not(hasDegreeGreater("movie", OUTGOING, literalInt(0))))
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = _ => GetValue
        )
        .build()
  }

  // TODO update apply to join after PLAN-3088
  test("plan node vector index search for previously bound nodes") {
    val planner = plannerBuilder().build()

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
        .filter("movie = movie", assertIsNode("movie"))
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "5",
          argumentIds = Set("d", "movie", "anon_0")
        )
        .filter("movie:Movie")
        .expandAll("(d)-[anon_0:DIRECTED]->(movie)")
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
        .projection("cacheN[movie.plot] AS `movie.plot`", "cacheN[movie2.plot] AS `movie2.plot`")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie2",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set("movie"),
          getValueFromIndex = { case "plot" => GetValue; case _ => DoNotGetValue }
        )
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = { case "plot" => GetValue; case _ => DoNotGetValue }
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
        .projection("cacheN[person.name] AS `person.name`", "cacheN[movie.plot] AS `movie.plot`")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set("person"),
          getValueFromIndex = { case "plot" => GetValue; case _ => DoNotGetValue }
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
        .projection("cacheN[movie.plot] AS `movie.plot`", "cacheN[movie2.plot] AS `movie2.plot`")
        .apply()
        .|.nodeVectorIndexSearch(
          node = "movie2",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "movie.embedding",
          limit = "10",
          argumentIds = Set("movie"),
          getValueFromIndex = { case "plot" => GetValue; case _ => DoNotGetValue }
        )
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = { case "plot" => GetValue; case _ => DoNotGetValue }
        )
        .build()
  }

  test("plan relationship vector index search with previously bound relationship") {
    val planner = plannerBuilder().build()

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
        .apply()
        .|.relationshipVectorIndexSearch(
          pattern = "()-[a]->()",
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
        .projection("cacheR[r.plot] AS `r.plot`")
        .filter(hasTypes("r", "CONTRIBUTED"))
        .cacheProperties("cacheRFromStore[r.plot]")
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

  private val comparisons = valuesToCompare.flatMap { case (cypher, expression) =>
    Seq(
      (s"IN [ $cypher ]", QueryExpressionConstructionTestSupport.single(expression)),
      (s"= $cypher", QueryExpressionConstructionTestSupport.single(expression)),
      (s"< $cypher", rangeExpression(lt(expression))),
      (s"<= $cypher", rangeExpression(lte(expression))),
      (s"> $cypher", rangeExpression(gt(expression))),
      (s">= $cypher", rangeExpression(gte(expression)))
    )
  } ++ Seq(
    ("IS NOT NULL", existsExpression),
    ("IS NULL", notExistsExpression)
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
            .produceResults(column(
              "movie",
              "cacheN[movie.plot]",
              "cacheN[movie.imdbRating]",
              "cacheN[movie.releaseYear]"
            ))
            .nodeVectorIndexSearch(
              node = "movie",
              labelNames = Seq("Movie"),
              properties = moviePlotsProperties,
              indexName = "moviePlots",
              vector = "$embedding",
              limit = "5",
              getValueFromIndex = moviePlotsProperties.map(_ -> GetValue).toMap,
              filter = Some(composite(comparisonExpression, all()))
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
        .produceResults(column("movie", "cacheN[movie.plot]", "cacheN[movie.imdbRating]", "cacheN[movie.releaseYear]"))
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "5",
          getValueFromIndex = moviePlotsProperties.map(_ -> GetValue).toMap,
          filter = Some(composite(
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
          the[VectorIndexSearchException] thrownBy {
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
                .produceResults(column("r", "cacheR[r.script]", "cacheR[r.workingDays]"))
                .relationshipVectorIndexSearch(
                  pattern = s"()${direction._1}[r]${direction._2}()",
                  typeNames = Seq("ACTS_IN"),
                  properties = properties,
                  indexName = "actsInScript",
                  vector = "$embedding",
                  limit = "5",
                  getValueFromIndex = properties.map(_ -> GetValue).toMap,
                  filter = Some(comparisonExpression)
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
      .projection("cacheN[movie.plot] AS `movie.plot`")
      .filter("cacheN[movie.releaseYear] > 2000")
      .nodeVectorIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = Seq("plot", "imdbRating", "releaseYear"),
        indexName = "moviePlots",
        vector = "$embedding",
        limit = "10",
        getValueFromIndex = Map("plot" -> GetValue, "imdbRating" -> DoNotGetValue, "releaseYear" -> GetValue),
        filter = Some(
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
      "cacheN[movie.imdbRating] = 8",
      QueryExpressionConstructionTestSupport.single(literalInt(8)),
      PlannerDefaults.DEFAULT_EQUALITY_SELECTIVITY * PlannerDefaults.DEFAULT_PROPERTY_SELECTIVITY
    )

    val releaseYearLessThan2010 = TestPredicate(
      "movie.releaseYear < 2010",
      "cacheN[movie.releaseYear] < 2010",
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
        imdbRatingPred.map(_.searchQuery).getOrElse(all()),
        releaseYearPred.map(_.searchQuery).getOrElse(all())
      )

      val query =
        s"""MATCH (movie:Movie)
           |${if (inlinePredicates) "" else whereClause}
           |SEARCH movie IN (
           |  VECTOR INDEX moviePlots
           |  FOR $$embedding
           |  ${if (inlinePredicates) whereClause else ""}
           |  LIMIT 10
           |)
           |RETURN movie.plot AS result""".stripMargin

      val expectedCardinality =
        movieLabelCardinality *
          Seq(imdbRatingPred, releaseYearPred)
            .flatten
            .map(_.selectivity.factor)
            .product

      val getValueFromIndex = Map(
        "plot" -> GetValue,
        "imdbRating" -> (if (inlinePredicates || imdbRatingPred.isEmpty) DoNotGetValue else GetValue),
        "releaseYear" -> (if (inlinePredicates || releaseYearPred.isEmpty) DoNotGetValue else GetValue)
      )

      val expectedPlanBuilder = planner.subPlanBuilder()
        .produceResults("result").withCardinality(expectedCardinality)
        .projection("cacheN[movie.plot] AS result").withCardinality(expectedCardinality)
        .planIf(!inlinePredicates)(_.filter(planExpressions: _*).withCardinality(expectedCardinality))
        .nodeVectorIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = Seq("plot", "imdbRating", "releaseYear"),
          indexName = "moviePlots",
          vector = "$embedding",
          limit = "10",
          getValueFromIndex = getValueFromIndex,
          filter = Option.when(inlinePredicates)(searchFilter)
        ).withCardinality(if (inlinePredicates) expectedCardinality else movieLabelCardinality)

      val actualPlanState = planner.planState(CypherVersion.Cypher25, query)

      withClue(s"query:\n$query\nexpectedCardinality: $expectedCardinality\n") {
        actualPlanState should haveSamePlanAndCardinalitiesAsBuilder(expectedPlanBuilder)
      }
    }
  }
}
