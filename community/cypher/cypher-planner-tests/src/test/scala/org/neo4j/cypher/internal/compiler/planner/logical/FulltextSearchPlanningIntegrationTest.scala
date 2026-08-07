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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeFulltextIndexSearch
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.exceptions.IndexSearchException
import org.neo4j.exceptions.InvalidArgumentException

class FulltextSearchPlanningIntegrationTest
    extends FulltextWithComplexPatternPlanningIntegrationTestBase

class FulltextWithComplexPatternPlanningIntegrationTest
    extends FulltextWithComplexPatternPlanningIntegrationTestBase

abstract class FulltextWithComplexPatternPlanningIntegrationTestBase
    extends FulltextSearchPlanningIntegrationTestBase {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .addSemanticFeature(SemanticFeature.VectorSearchWithComplexPattern)

  // used to break cost ties in cartesian products
  private val smallBatchedExecutionModel = ExecutionModel.BatchedSingleThreaded(2, 4)

  test("plan node fulltext index search with pattern containing multiple variable declarations") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie.plot as plot, director.name as name""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.plot AS plot", "director.name AS name")
      .filter("director:Person")
      .expandAll("(movie)<-[:DIRECTED]-(director)")
      .nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = "'magic genie'",
        limit = "10",
        argumentIds = Set()
      )
      .build()
  }

  test("plan node fulltext index search with pattern containing multiple relationships") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (a)-[r]->(b)-[p]->(movie:Movie)-[q]->(c)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie.plot as plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.plot AS plot")
      .filter("NOT q = r", "NOT p = r")
      .expandAll("(b)<-[r]-()")
      .filter("NOT q = p")
      .expandAll("(movie)<-[p]-(b)")
      .expandAll("(movie)-[q]->()")
      .nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = "'magic genie'",
        limit = "10",
        argumentIds = Set()
      )
      .build()
  }

  test("plan node fulltext index search with comma-separated pattern, single component") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (a)-[p:CONTRIBUTED]->(movie:Movie)-[q]->(b), (movie)<-[r:ACTS_IN]-(c)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie.plot as plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.plot AS plot")
      .filter("NOT q = r")
      .expandAll("(movie)<-[r:ACTS_IN]-()")
      .filter("NOT q = p")
      .expandAll("(movie)-[q]->()")
      .expandAll("(movie)<-[p:CONTRIBUTED]-()")
      .nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = "'magic genie'",
        limit = "10",
        argumentIds = Set()
      )
      .build()
  }

  test("plan node fulltext index search with comma-separated pattern, multiple components") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (a)-[p:CONTRIBUTED]->(movie:Movie)-[q]->(b), (otherMovie)<-[r:ACTS_IN]-(c)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |WHERE movie.year = otherMovie.year
        |RETURN movie.plot as plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[movie.plot] AS plot")
      .filter("NOT q = r", "cacheN[movie.year] = otherMovie.year")
      .cartesianProduct()
      .|.relationshipTypeScan("()-[r:ACTS_IN]->(otherMovie)")
      .cacheProperties("cacheNFromStore[movie.year]", "cacheNFromStore[movie.plot]")
      .filter("NOT q = p")
      .expandAll("(movie)-[q]->()")
      .expandAll("(movie)<-[p:CONTRIBUTED]-()")
      .nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = "'magic genie'",
        limit = "10",
        argumentIds = Set()
      )
      .build()
  }

  test("plan node fulltext index search with QPP") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) ((x:Movie)<-[p:DIRECTED]-(dir)-[q:DIRECTED]->(y:Movie)){1, 4} (otherMovie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |WHERE movie.year = otherMovie.year
        |RETURN movie.plot as plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    val trailParameters = TrailParameters(
      min = 1,
      max = UpperBound.Limited(4),
      start = "movie",
      end = "otherMovie",
      innerStart = "x",
      innerEnd = "y",
      groupNodes = Set(),
      groupRelationships = Set(),
      innerRelationships = Set("p", "q"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set()
    )
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.plot AS plot")
      .filter("movie.year = otherMovie.year", "otherMovie:Movie")
      .repeatTrail(trailParameters)
      .|.filter("NOT q = p", isRepeatTrailUnique("q"), "y:Movie")
      .|.expandAll("(dir)-[q:DIRECTED]->(y)")
      .|.filter(isRepeatTrailUnique("p"))
      .|.expandAll("(x)<-[p:DIRECTED]-(dir)")
      .|.filter("x:Movie")
      .|.argument("x")
      .nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = "'magic genie'",
        limit = "10",
        argumentIds = Set()
      )
      .build()
  }

  test("plan node fulltext index search with var-length relationship") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)-[r*2..4]->(endNode)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie.plot as plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.plot AS plot")
      .expand("(movie)-[*2..4]->()")
      .nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = "'magic genie'",
        limit = "10",
        argumentIds = Set()
      )
      .build()
  }

  test("plan node fulltext index search with query string depending on a symbol from the same pattern") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR director.name
        |    LIMIT 10
        |  )
        |RETURN movie.plot as plot, director.name as name""".stripMargin

    val (plan, costComparisonCandidates) = planner.planAndRecordCostComparisonCandidates(CypherVersion.Cypher25, query)

    plan.stripProduceResults shouldEqual planner.subPlanBuilder()
      .projection("cacheN[movie.plot] AS plot", "cacheN[director.name] AS name")
      .apply()
      .|.cacheProperties("cacheNFromStore[movie.plot]")
      .|.nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = cachedNodeProp("director", "name"),
        limit = "10",
        argumentIds = Set("director", "movie")
      )
      .filter("movie:Movie")
      .expandAll("(director)-[:DIRECTED]->(movie)")
      .cacheProperties("cacheNFromStore[director.name]")
      .nodeByLabelScan("director", "Person")
      .build()

    costComparisonCandidates should contain {
      planner.subPlanBuilder()
        .expandInto("(director)-[rel:DIRECTED]->(movie)")
        .apply()
        .|.nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          queryString = prop("director", "name"),
          limit = "10",
          argumentIds = Set("director")
        )
        .nodeByLabelScan("director", "Person")
        .build()
    }
  }

  test(
    "plan node fulltext index search with query string depending on a symbol from the same pattern, longer pattern"
  ) {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .build()

    val query =
      """MATCH (movie:Movie)<-[otherRel]-(otherMovie)<-[rel:DIRECTED]-(director:Person)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR director.name
        |    LIMIT 10
        |  )
        |RETURN movie.plot as plot, director.name as name""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("movie.plot AS plot", "cacheN[director.name] AS name")
      .filter("NOT rel = otherRel")
      .expandInto("(otherMovie)<-[rel:DIRECTED]-(director)")
      .expandAll("(movie)<-[otherRel]-(otherMovie)")
      .apply()
      .|.nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = "cacheNFromStore[director.name]",
        limit = "10",
        argumentIds = Set("director")
      )
      .nodeByLabelScan("director", "Person")
      .build()
  }

  test("plan node fulltext index search with a pattern with multiple components") {
    val planner = plannerBuilder()
      .setExecutionModel(smallBatchedExecutionModel)
      .build()

    val query =
      """MATCH (n), (movie:Movie), (m:Person)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie.plot as plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[movie.plot] AS plot")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.cacheProperties("cacheNFromStore[movie.plot]")
      .|.|.nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = "'magic genie'",
        limit = "10"
      )
      .|.allNodeScan("n")
      .nodeByLabelScan("m", "Person")
      .build()
  }

  test("plan node fulltext index search with a pattern with multiple components, query string from another component") {
    val planner = plannerBuilder()
      .enableDeduplicateNames(false)
      .setExecutionModel(smallBatchedExecutionModel)
      .build()

    val query =
      """MATCH (n), (movie:Movie), (m:Person)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR n.prop
        |    LIMIT 10
        |  )
        |RETURN movie.plot as plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[movie.plot] AS plot")
      .cartesianProduct()
      .|.nodeByLabelScan("m", "Person")
      .apply()
      .|.cacheProperties("cacheNFromStore[movie.plot]")
      .|.nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = moviePlotsProperties,
        indexName = "moviePlots",
        queryString = cachedNodeProp("n", "prop"),
        limit = "10",
        argumentIds = Set("n", "movie")
      )
      .cartesianProduct()
      .|.nodeByLabelScan("movie", "Movie")
      .cacheProperties("cacheNFromStore[n.prop]")
      .allNodeScan("n")
      .build()
  }
}

abstract class FulltextSearchPlanningIntegrationTestBase extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanConstructionTestSupport
    with LogicalPlanningAttributesTestSupport
    with QueryExpressionConstructionTestSupport {

  protected def movieLabelCardinality: Double = 80.0

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .addSemanticFeature(SemanticFeature.FulltextSearch)
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
      .addNodeFulltextIndex("moviePlots", Seq("Movie"), Seq("title", "plot"))
      .addNodeFulltextIndex("movieOrDirectorInfo", Seq("Movie", "Director"), Seq("info"))
      .addNodeIndex("Movie", List("title"), 1.0, 1.0 / 120.0, isUnique = true)
      .addRelationshipFulltextIndex("actsInScript", Seq("ACTS_IN"), Seq("script", "notes"))
      .addRelationshipFulltextIndex("contributed", Seq("CONTRIBUTED"), Seq("summary"))
      .addRelationshipFulltextIndex("actsOrContributedInScript", Seq("ACTS_IN", "CONTRIBUTED"), Seq("script"))

  protected val moviePlotsProperties: Seq[String] = Seq("title", "plot")
  protected val actsInScriptProperties: Seq[String] = Seq("script", "notes")

  test(
    "plan node fulltext index search with a literal query string"
  ) {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`")
        .projection("movie.plot AS `movie.plot`")
        .nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          queryString = "'magic genie'",
          limit = "10",
          argumentIds = Set(),
          getValueFromIndex = _ => DoNotGetValue
        )
        .build()
  }

  test("plan node fulltext index search with a parameter query string") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR $query
        |    LIMIT 10
        |  )
        |RETURN movie.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`")
        .projection("movie.plot AS `movie.plot`")
        .nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          queryString = "$query",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test("plan node fulltext index search with a query string referencing a previously bound variable") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 120)
      .build()

    val query =
      """MATCH (person:Person)
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR person.bio
        |    LIMIT 10
        |  )
        |RETURN movie, person""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie", "person")
        .apply()
        .|.nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          queryString = "person.bio",
          limit = "10",
          argumentIds = Set("person")
        )
        .nodeByLabelScan("person", "Person")
        .build()
  }

  test("plan node fulltext index search with SKIP") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    SKIP 5
        |    LIMIT 10
        |  )
        |RETURN movie.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`")
        .projection("movie.plot AS `movie.plot`")
        .nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          queryString = "'magic genie'",
          limit = "10",
          skip = Some("5"),
          argumentIds = Set()
        )
        .build()
  }

  test("plan node fulltext index search with WITH ANALYZER") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie' WITH ANALYZER 'english'
        |    LIMIT 10
        |  )
        |RETURN movie.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`")
        .projection("movie.plot AS `movie.plot`")
        .nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          queryString = "'magic genie'",
          limit = "10",
          analyzer = Some("'english'"),
          argumentIds = Set()
        )
        .build()
  }

  test("plan node fulltext index search projecting the score variable") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  ) SCORE AS relevance
        |RETURN movie, relevance""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("movie", "relevance")
        .nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          queryString = "'magic genie'",
          limit = "10",
          score = "relevance",
          argumentIds = Set()
        )
        .build()
  }

  test("plan directed relationship fulltext index search") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->()
        |  SEARCH r IN (
        |    FULLTEXT INDEX actsInScript
        |    FOR 'thrilling chase'
        |    LIMIT 10
        |  )
        |RETURN r.script""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.script`")
        .projection("r.script AS `r.script`")
        .relationshipFulltextIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN"),
          properties = actsInScriptProperties,
          indexName = "actsInScript",
          queryString = "'thrilling chase'",
          limit = "10",
          getValueFromIndex = _ => DoNotGetValue
        )
        .build()
  }

  test("plan undirected relationship fulltext index search") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[c]-()
        |  SEARCH c IN (
        |    FULLTEXT INDEX contributed
        |    FOR 'screenplay notes'
        |    LIMIT 10
        |  )
        |RETURN c.summary""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`c.summary`")
        .projection("c.summary AS `c.summary`")
        .relationshipFulltextIndexSearch(
          pattern = "()-[c]-()",
          typeNames = Seq("CONTRIBUTED"),
          properties = Seq("summary"),
          indexName = "contributed",
          queryString = "'screenplay notes'",
          limit = "10"
        )
        .build()
  }

  test("plan relationship fulltext index search using a multi-type index") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->()
        |  SEARCH r IN (
        |    FULLTEXT INDEX actsOrContributedInScript
        |    FOR 'thrilling chase'
        |    LIMIT 10
        |  )
        |RETURN r.script""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.script`")
        .projection("r.script AS `r.script`")
        .relationshipFulltextIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN", "CONTRIBUTED"),
          properties = Seq("script"),
          indexName = "actsOrContributedInScript",
          queryString = "'thrilling chase'",
          limit = "10"
        )
        .build()
  }

  test("residual predicates land in a Filter while the single-label predicate is implicitly solved by the leaf") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) WHERE movie.title = 'Inception'
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie.plot""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.plot`")
        .projection("movie.plot AS `movie.plot`")
        .filter("movie.title = 'Inception'")
        .nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          queryString = "'magic genie'",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test("plan node fulltext index search using a multi-label index and filter on the specified label") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) WHERE movie.info IS NOT NULL
        |  SEARCH movie IN (
        |    FULLTEXT INDEX movieOrDirectorInfo
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie.info""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.info`")
        .projection("movie.info AS `movie.info`")
        .filter("movie:Movie") // by contrast, info IS NOT NULL is implicitly solved as it's the index's only property
        .nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie", "Director"),
          properties = Seq("info"),
          indexName = "movieOrDirectorInfo",
          queryString = "'magic genie'",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test("multi-property node index leaves IS NOT NULL on an indexed property to a Filter") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie) WHERE movie.title IS NOT NULL
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie.title""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`movie.title`")
        .projection("cacheN[movie.title] AS `movie.title`")
        // The match could have come from plot alone, so title IS NOT NULL is not implied by the index.
        .filter("cacheNFromStore[movie.title] IS NOT NULL")
        .nodeFulltextIndexSearch(
          node = "movie",
          labelNames = Seq("Movie"),
          properties = moviePlotsProperties,
          indexName = "moviePlots",
          queryString = "'magic genie'",
          limit = "10",
          argumentIds = Set()
        )
        .build()
  }

  test("single-property relationship index implicitly solves IS NOT NULL on the indexed property") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[c:CONTRIBUTED]->() WHERE c.summary IS NOT NULL
        |  SEARCH c IN (
        |    FULLTEXT INDEX contributed
        |    FOR 'screenplay notes'
        |    LIMIT 10
        |  )
        |RETURN c.summary""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`c.summary`")
        .projection("c.summary AS `c.summary`")
        .relationshipFulltextIndexSearch(
          pattern = "()-[c:CONTRIBUTED]->()",
          typeNames = Seq("CONTRIBUTED"),
          properties = Seq("summary"),
          indexName = "contributed",
          queryString = "'screenplay notes'",
          limit = "10"
        )
        .build()
  }

  test("multi-property relationship index leaves IS NOT NULL on an indexed property to a Filter") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r:ACTS_IN]->() WHERE r.script IS NOT NULL
        |  SEARCH r IN (
        |    FULLTEXT INDEX actsInScript
        |    FOR 'thrilling chase'
        |    LIMIT 10
        |  )
        |RETURN r.script""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.script`")
        .projection("cacheR[r.script] AS `r.script`")
        // The match could have come from notes alone, so script IS NOT NULL is not implied by the index.
        .filter("cacheRFromStore[r.script] IS NOT NULL")
        .relationshipFulltextIndexSearch(
          pattern = "()-[r:ACTS_IN]->()",
          typeNames = Seq("ACTS_IN"),
          properties = actsInScriptProperties,
          indexName = "actsInScript",
          queryString = "'thrilling chase'",
          limit = "10"
        )
        .build()
  }

  test("plan relationship fulltext index search using a multi-type index and filter on the specified type") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r:ACTS_IN]->() WHERE r.script IS NOT NULL
        |  SEARCH r IN (
        |    FULLTEXT INDEX actsOrContributedInScript
        |    FOR 'thrilling chase'
        |    LIMIT 10
        |  )
        |RETURN r.script""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.script`")
        .projection("cacheR[r.script] AS `r.script`")
        .filter("r:ACTS_IN") // by contrast, script IS NOT NULL is implicitly solved as it's the index's only property
        .cacheProperties("cacheRFromStore[r.script]")
        .relationshipFulltextIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN", "CONTRIBUTED"),
          properties = Seq("script"),
          indexName = "actsOrContributedInScript",
          queryString = "'thrilling chase'",
          limit = "10"
        )
        .build()
  }

  test("a type predicate allowing every type the multi-type index covers needs no Filter") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->() WHERE r:ACTS_IN OR r:CONTRIBUTED
        |  SEARCH r IN (
        |    FULLTEXT INDEX actsOrContributedInScript
        |    FOR 'thrilling chase'
        |    LIMIT 10
        |  )
        |RETURN r.script""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.script`")
        .projection("r.script AS `r.script`")
        .relationshipFulltextIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN", "CONTRIBUTED"),
          properties = Seq("script"),
          indexName = "actsOrContributedInScript",
          queryString = "'thrilling chase'",
          limit = "10"
        )
        .build()
  }

  test("plan relationship fulltext index search using a multi-property index and read both properties from the store") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r]->()
        |  SEARCH r IN (
        |    FULLTEXT INDEX actsInScript
        |    FOR 'thrilling chase'
        |    LIMIT 10
        |  )
        |RETURN r.script, r.notes""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("`r.script`", "`r.notes`")
        .projection("cacheR[r.script] AS `r.script`", "cacheR[r.notes] AS `r.notes`")
        .cacheProperties("cacheRFromStore[r.script]", "cacheRFromStore[r.notes]")
        .relationshipFulltextIndexSearch(
          pattern = "()-[r]->()",
          typeNames = Seq("ACTS_IN"),
          properties = actsInScriptProperties,
          indexName = "actsInScript",
          queryString = "'thrilling chase'",
          limit = "10"
        )
        .build()
  }

  test("should fail with an index-not-found error when the fulltext index does not exist") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX doesNotExist
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie""".stripMargin

    val caughtException = intercept[IndexSearchException] {
      planner.plan(CypherVersion.Cypher25, query)
    }
    caughtException.gqlStatus() should be("22N69")
  }

  test("should fail with a wrong-index-type error when the name refers to a vector index") {
    val planner =
      plannerBuilder()
        .addNodeVectorIndex("movieEmbeddings", Seq("Movie"), "embedding")
        .build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX movieEmbeddings
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie""".stripMargin

    val caughtException = intercept[InvalidArgumentException] {
      planner.plan(CypherVersion.Cypher25, query)
    }
    caughtException.gqlStatus() should be("22NCG")
  }

  test("should fail with a wrong-binding-variable-type error when a relationship is searched against a node index") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH ()-[r:ACTS_IN]->()
        |  SEARCH r IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN r""".stripMargin

    val caughtException = intercept[IndexSearchException] {
      planner.plan(CypherVersion.Cypher25, query)
    }
    caughtException.gqlStatus() should be("22G03")
    caughtException.legacyMessage() should be(
      "22N01: Expected the value `r` to be of type NODE, but was of type RELATIONSHIP."
    )
  }

  test("should fail with an index-in-populating-state error when the fulltext index is populating") {
    val planner =
      plannerBuilder()
        .addNodeFulltextIndex("populatingMoviePlots", Seq("Movie"), Seq("title", "plot"))
        .addPopulatingIndex("populatingMoviePlots")
        .build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX populatingMoviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |RETURN movie""".stripMargin

    val caughtException = intercept[IndexSearchException] {
      planner.plan(CypherVersion.Cypher25, query)
    }
    caughtException.gqlStatus() should be("51N63")
    caughtException.legacyMessage() should be(
      "51N63: Index `populatingMoviePlots` is not ready yet. Wait until it finishes populating and retry the transaction."
    )
  }

  test("the fulltext leaf is the only leaf candidate for the search component while other query parts plan normally") {
    val planner = plannerBuilder().build()

    val query =
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    FULLTEXT INDEX moviePlots
        |    FOR 'magic genie'
        |    LIMIT 10
        |  )
        |MATCH (person:Person)
        |RETURN movie, person""".stripMargin

    val (plan, costComparisonCandidates) = planner.planAndRecordCostComparisonCandidates(CypherVersion.Cypher25, query)

    plan shouldEqual planner.planBuilder()
      .produceResults("movie", "person")
      .apply()
      .|.nodeByLabelScan("person", "Person", IndexOrderNone, "movie")
      .nodeFulltextIndexSearch(
        node = "movie",
        labelNames = Seq("Movie"),
        properties = Seq("title", "plot"),
        indexName = "moviePlots",
        queryString = "'magic genie'",
        limit = "10",
        analyzer = None,
        skip = None,
        argumentIds = Set(),
        getValueFromIndex = Map("title" -> DoNotGetValue, "plot" -> DoNotGetValue)
      )
      .build()

    // The fulltext leaf is the only candidate for the search component, while the other query parts plan normally.
    costComparisonCandidates.foreach { candidate =>
      candidate.availableSymbols match {
        case s if s == Set(v"movie")  => candidate shouldBe a[NodeFulltextIndexSearch]
        case s if s == Set(v"person") => candidate shouldBe a[NodeByLabelScan]
        case _                        =>
      }
    }
  }
}
