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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.VectorSearchClause
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.scalatest.OptionValues

class SearchConverterTest extends SearchConverterTestBase
class SearchWithComplexPatternConverterTest extends SearchWithComplexPatternConverterTestBase

abstract class SearchConverterTestBase extends CypherPlannerTestSuite with LogicalPlanningTestSupport
    with OptionValues {

  protected def buildSinglePlannerQuery(query: String, version: CypherVersion): SinglePlannerQuery =
    buildPlannerQuery(
      version = version,
      query = query,
      procedureLookup = None,
      functionLookup = None,
      compareVersions = false,
      additionalSettings = Map.empty
    ).asSinglePlannerQuery

  test("should split MATCH-SEARCH off from other clauses taking the search clause with it") {
    versionsExcept5 { version =>
      val query =
        """MATCH (m:Movie)
          |MATCH (movie:Movie)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR m.embedding
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title """.stripMargin

      val matchSearchQG = buildSinglePlannerQuery(query, version)
        .allPlannerQueries
        .map(_.queryGraph)
        .find(_.patternNodes.contains(varFor("movie")))
        .getOrElse(fail("Expected to find query graph with movie node"))

      matchSearchQG.selections.flatPredicates shouldEqual
        Seq(hasLabels("movie", "Movie"))

      matchSearchQG.searchClause.get shouldEqual
        VectorSearchClause(
          resultVariable = v"movie",
          indexName = "moviePlots",
          embedding = prop("m", "embedding"),
          None,
          limit = SignedDecimalIntegerLiteral("5")(pos),
          scoreVariable = None
        )
    }
  }

  test("score variable should be mapped to the search clause") {
    versionsExcept5 { version =>
      val query =
        """MATCH (m:Movie)
          |MATCH (movie:Movie)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR m.embedding
          |    LIMIT 5
          |  ) SCORE as score
          |RETURN score, movie.title AS title """.stripMargin

      val searchClause = buildSinglePlannerQuery(query, version)
        .allPlannerQueries
        .map(_.queryGraph)
        .find(_.patternNodes.contains(varFor("movie")))
        .flatMap(_.searchClause)
        .getOrElse(fail("Expected search clause for MATCH-SEARCH clause"))

      searchClause shouldEqual
        VectorSearchClause(
          resultVariable = v"movie",
          indexName = "moviePlots",
          embedding = prop("m", "embedding"),
          None,
          limit = SignedDecimalIntegerLiteral("5")(pos),
          scoreVariable = Some(v"score")
        )
    }
  }

  test("MATCH-SEARCH clauses should be in separate query graph to other clauses") {
    versionsExcept5 { version =>
      val query =
        """
          |MATCH (movie:Movie) WHERE movie.plot IS NOT NULL
          |LET embedding = [0.1, 0.2, -0.3]
          |MATCH (movie)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR embedding
          |    LIMIT 10
          |  ) SCORE AS similarity
          |MATCH (movie)<-[:DIRECTED]-+(person:Person)
          |WHERE similarity > 0.8
          |RETURN person.name, movie.plot""".stripMargin

      val queryGraphs =
        buildSinglePlannerQuery(query, version)
          .allPlannerQueries
          .map(_.queryGraph)

      queryGraphs.size shouldEqual 3

      val queryGraphsWithSearchClause = queryGraphs.filter(_.searchClause.nonEmpty)

      val searchClause =
        queryGraphsWithSearchClause
          .head
          .searchClause
          .getOrElse(fail("Expected search clause"))

      searchClause shouldEqual
        VectorSearchClause(
          resultVariable = v"movie",
          indexName = "moviePlots",
          embedding = v"embedding",
          where = None,
          limit = SignedDecimalIntegerLiteral("10")(pos),
          scoreVariable = Some(v"similarity")
        )
    }
  }

  test("search clause should be correctly formatted as string") {
    versionsExcept5 { version =>
      val query =
        """MATCH (m:Movie)
          |MATCH (movie:Movie)
          |  search movie in (
          |    vector index moviePlots
          |    for m.embedding
          |    where movie.prop > 10
          |      and movie.prop <= 100
          |    limit 5
          |  ) score as similarity
          |RETURN similarity, movie.title AS title """.stripMargin

      val searchClauseAsString = buildSinglePlannerQuery(query, version)
        .allPlannerQueries
        .map(_.queryGraph)
        .find(_.patternNodes.contains(varFor("movie")))
        .flatMap(_.searchClause)
        .getOrElse(fail("Expected search clause for MATCH-SEARCH clause"))
        .toString

      searchClauseAsString shouldEqual
        "SEARCH movie IN (VECTOR INDEX moviePlots FOR m.embedding WHERE movie.prop > 10 AND movie.prop <= 100 LIMIT 5) SCORE AS similarity"
    }
  }

  test("should not include predicates inlined in SEARCH in selections") {
    versionsExcept5 { version =>
      val query =
        """MATCH (m:Movie)
          |MATCH (movie:Movie)
          |WHERE movie.rating > 6 AND movie.year > 2000
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR m.embedding
          |    WHERE movie.rating > 6
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title """.stripMargin

      val qg = buildSinglePlannerQuery(query, version).tail.value.queryGraph

      qg.selections.flatPredicates shouldEqual Seq(
        hasLabels("movie", "Movie"),
        greaterThan(prop("movie", "year"), literal(2000))
      )

      qg.searchClause.value shouldEqual VectorSearchClause(
        resultVariable = v"movie",
        indexName = "moviePlots",
        embedding = prop("m", "embedding"),
        where = Some(where(greaterThan(prop("movie", "rating"), literal(6)))),
        limit = literal(5),
        scoreVariable = None
      )
    }
  }

  test("should not insert an empty planner query before MATCH-SEARCH") {
    versionsExcept5 { version =>
      val query =
        """MATCH (movie:Movie)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title """.stripMargin

      val pq = buildSinglePlannerQuery(query, version)

      pq shouldEqual
        SinglePlannerQuery.empty
          .withQueryGraph(
            QueryGraph.empty
              .addPatternNodes(v"movie")
              .addPredicates(hasLabels(v"movie", "Movie"))
              .addSearchClause(Some(VectorSearchClause(
                resultVariable = v"movie",
                indexName = "moviePlots",
                embedding = parameter("param", CTAny),
                where = None,
                limit = literal(5),
                scoreVariable = None
              )))
          )
          .withHorizon(
            QueryProjection.empty
              .withAddedProjections(Map(v"title" -> prop(v"movie", "title")))
              .markAsFinal
          )
    }
  }

  test("should not insert an empty planner query before MATCH-SEARCH inside CALL") {
    versionsExcept5 { version =>
      val query =
        """MATCH (n)
          |CALL (n) {
          |  MATCH (movie:Movie)
          |    SEARCH movie IN (
          |      VECTOR INDEX moviePlots
          |      FOR n.embedding
          |      LIMIT 5
          |    )
          |  RETURN movie.title AS title
          |}
          |RETURN title""".stripMargin

      val pq = buildSinglePlannerQuery(query, version)
      pq.horizon shouldEqual CallSubqueryHorizon(
        correlated = true,
        yielding = true,
        inTransactionsParameters = None,
        optional = false,
        importedVariables = Set(v"n"),
        importedSymbolsFromLastCallSubquery = Set.empty,
        callSubquery = SinglePlannerQuery.empty
          .withQueryGraph(
            QueryGraph.empty
              .addArgumentId(v"n")
              .addPatternNodes(v"movie")
              .addPredicates(hasLabels(v"movie", "Movie"))
              .addSearchClause(Some(VectorSearchClause(
                resultVariable = v"movie",
                indexName = "moviePlots",
                embedding = prop(v"n", "embedding"),
                where = None,
                limit = literal(5),
                scoreVariable = None
              )))
          )
          .withHorizon(
            QueryProjection.empty
              .withAddedProjections(Map(v"title" -> prop(v"movie", "title")))
              .withImportedExposedSymbols(Set(v"n"))
          )
      )
    }
  }
}

abstract class SearchWithComplexPatternConverterTestBase extends SearchConverterTestBase {

  override def semanticFeatures: List[SemanticFeature] =
    super.semanticFeatures.prepended(SemanticFeature.VectorSearchWithComplexPattern)

  test("should convert MATCH-SEARCH with complex pattern") {
    val complexPatterns = Seq(
      // multiple variable declarations
      "(movie:Movie)<-[rel:DIRECTED]-(director:Person)",
      // multiple relationship patterns
      "()-->()-->(movie:Movie)-->()",
      // graph patterns
      "()-->(movie:Movie)-->(), (movie)-->()",
      "()-->(movie:Movie)-->(), (a)-->(b)",
      // QPP
      "(start) ((x)-->(y) WHERE x.prop > y.prop){1, 4} (movie:Movie)-->()",
      // var-length
      "(movie)-[r*1..4]->(person:Person)",
      // node pattern predicate
      "(movie)-[r]->(person:Person WHERE person.name = 'Alice')"
    )

    for {
      version <- versionsExcept5Iterable
      pattern <- complexPatterns
    } withClue(s"CYPHER $version\npattern: $pattern\n") {
      val noSearchQuery =
        s"""
           |MATCH $pattern
           |RETURN movie.title AS title
           |""".stripMargin

      val searchQuery =
        s"""
           |MATCH $pattern
           |  SEARCH movie IN (
           |    VECTOR INDEX moviePlots
           |    FOR $$param
           |    LIMIT 5
           |  )
           |RETURN movie.title AS title
           |""".stripMargin

      val noSearchPlannerQuery = buildSinglePlannerQuery(noSearchQuery, version)
      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)

      noSearchPlannerQuery.queryGraph.searchClause shouldEqual None

      searchPlannerQuery.queryGraph.searchClause.value shouldEqual
        VectorSearchClause(
          resultVariable = v"movie",
          indexName = "moviePlots",
          embedding = parameter("param", CTAny),
          where = None,
          limit = literal(5),
          scoreVariable = None
        )

      searchPlannerQuery.amendQueryGraph(_.withoutSearchClause) shouldEqual noSearchPlannerQuery
    }
  }
}
