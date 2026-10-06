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
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.VarLengthLowerBound
import org.neo4j.cypher.internal.expressions.VarLengthUpperBound
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PassthroughAllHorizon
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.VectorSearchClause
import org.neo4j.cypher.internal.ir.VectorSearchClause.MatchFilterConfig
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.scalatest.Inside
import org.scalatest.OptionValues

class SearchConverterTest extends SearchConverterTestBase
class SearchWithComplexPatternConverterTest extends SearchWithComplexPatternConverterTestBase

abstract class SearchConverterTestBase extends CypherPlannerTestSuite with LogicalPlanningTestSupport
    with OptionValues {

  private def buildSinglePlannerQuery(query: String, version: CypherVersion): SinglePlannerQuery =
    buildPlannerQuery(
      version = version,
      query = query,
      procedureLookup = None,
      functionLookup = None,
      compareVersions = false,
      additionalSettings = Map.empty,
      enableNameDeduplication = true
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
          scoreVariable = None,
          matchFilterConfig = None
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
          scoreVariable = Some(v"score"),
          matchFilterConfig = None
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
          scoreVariable = Some(v"similarity"),
          matchFilterConfig = None
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
        scoreVariable = None,
        matchFilterConfig = None
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
                scoreVariable = None,
                matchFilterConfig = None
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
                scoreVariable = None,
                matchFilterConfig = None
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

  test("should not classify a regular CALL subquery as MATCH-filter subquery") {
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
      pq.horizon match {
        case horizon @ SearchWithComplexPatternConverter.MatchFilterSubquery(_) =>
          fail(s"Did not expect MATCH-filter subquery horizon, got: $horizon")
        case expected: CallSubqueryHorizon => ()
        case other                         => fail(s"Expected CallSubqueryHorizon, got: $other")
      }
    }
  }
}

abstract class SearchWithComplexPatternConverterTestBase extends SearchConverterTestBase with Inside {

  override def semanticFeatures: List[SemanticFeature] =
    super.semanticFeatures.prepended(SemanticFeature.VectorSearchWithComplexPattern)

  private def buildSinglePlannerQuery(query: String, version: CypherVersion): SinglePlannerQuery =
    buildPlannerQuery(
      version = version,
      query = query,
      procedureLookup = None,
      functionLookup = None,
      compareVersions = false,
      additionalSettings = Map.empty,
      enableNameDeduplication = false
    ).asSinglePlannerQuery

  extension (v: Variable) {

    def at(pos: Int): LogicalVariable = {
      varFor(
        AnonymousVariableNameGenerator.includeName(v.name, AnonymousVariableNameGenerator.anonymousVarName(pos))
      )
    }
  }

  private def anonVar(pos: Int): LogicalVariable = {
    varFor(AnonymousVariableNameGenerator.anonymousVarName(pos))
  }

  private def assertCanExtractMatchFilterSubquery(searchPlannerQuery: SinglePlannerQuery): Unit = {
    val callSubqueryHorizons = searchPlannerQuery.folder.findAllByClass[CallSubqueryHorizon]
    callSubqueryHorizons should not be empty
    callSubqueryHorizons.exists(
      SearchWithComplexPatternConverter.MatchFilterSubquery.unapply(_).isDefined
    ) shouldBe true
  }

  test("multiple variable declarations") {
    versionsExcept5 { version =>
      val searchQuery =
        """
          |MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)

      val director = v"director"
      val movie = v"movie"
      val rel = v"rel"

      val director0 = v"inner_director".at(0)
      val movie1 = v"inner_movie".at(1)
      val rel2 = v"inner_rel".at(2)
      val movie3 = movie.at(3)
      val candidateIds4 = v"candidateIds".at(4)
      val groups5 = v"groups".at(5)
      val group6 = v"group".at(6)
      val rows7 = v"rows".at(7)
      val record8 = v"record".at(8)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          horizon = CallSubqueryHorizon(
            correlated = false,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set.empty,
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(
                patternNodes = Set(director0, movie1),
                patternRelationships = Set(PatternRelationship(
                  rel2,
                  (movie1, director0),
                  SemanticDirection.INCOMING,
                  Seq(relTypeName("DIRECTED")),
                  SimplePatternLength
                )),
                selections = Selections.from(Set(hasLabels(director0, "Person"), hasLabels(movie1, "Movie")))
              ),
              horizon = AggregatingQueryProjection(
                groupingExpressions = Map(movie1 -> movie1),
                aggregationExpressions =
                  Map(rows7 -> collect(mapOf(director0.name -> director0, movie1.name -> movie1, rel2.name -> rel2)))
              ),
              tail = Some(
                RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(argumentIds = Set(movie1, rows7)),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map.empty,
                    aggregationExpressions = Map(
                      candidateIds4 -> compileEntityFilter(id(movie1), "moviePlots"),
                      groups5 -> collect(mapOf(movie1.name -> movie1, rows7.name -> rows7))
                    )
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(
                        patternNodes = Set(movie3),
                        argumentIds = Set(candidateIds4, groups5),
                        searchClause = Some(VectorSearchClause(
                          resultVariable = movie3,
                          indexName = "moviePlots",
                          embedding = parameter("param", CTAny),
                          where = None,
                          limit = literal(5),
                          scoreVariable = None,
                          matchFilterConfig = Some(MatchFilterConfig(
                            candidateIds = candidateIds4,
                            groupsCollection = groups5,
                            group = group6,
                            joinPredicate = equals(prop(group6, movie1.name), movie3)
                          ))
                        ))
                      ),
                      horizon = UnwindProjection(
                        variable = record8,
                        exp = prop(group6, rows7.name),
                        importedSymbolsFromLastCallSubquery = Set.empty
                      ),
                      tail = Some(RegularSinglePlannerQuery(
                        queryGraph = QueryGraph(argumentIds = Set(candidateIds4, groups5, movie3, record8)),
                        horizon = RegularQueryProjection(projections =
                          Map(
                            director -> prop(record8, director0.name),
                            movie -> prop(record8, movie1.name),
                            rel -> prop(record8, rel2.name)
                          )
                        )
                      ))
                    )
                  )
                )
              )
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(director, movie, rel)),
            horizon = RegularQueryProjection(
              projections = Map(v"title" -> prop(movie, "title")),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("multiple relationship patterns") {
    versionsExcept5 { version =>
      val searchQuery =
        """
          |MATCH ()-->()-->(movie:Movie)-->()
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)

      val movie = v"movie"
      val rows = v"rows"

      val anonNode0 = anonVar(0)
      val anonRel1 = anonVar(1)
      val anonNode2 = anonVar(2)
      val anonRel3 = anonVar(3)
      val anonRel4 = anonVar(4)
      val anonNode5 = anonVar(5)
      val movie6 = v"inner_movie".at(6)
      val movie7 = movie.at(7)
      val candidateIds8 = v"candidateIds".at(8)
      val groups9 = v"groups".at(9)
      val group10 = v"group".at(10)
      val rows11 = rows.at(11)
      val record12 = v"record".at(12)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          horizon = CallSubqueryHorizon(
            correlated = false,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set.empty,
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(
                patternNodes = Set(anonNode0, anonNode2, movie6, anonNode5),
                patternRelationships = Set(
                  PatternRelationship(
                    anonRel1,
                    (anonNode0, anonNode2),
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    SimplePatternLength
                  ),
                  PatternRelationship(
                    anonRel3,
                    (anonNode2, movie6),
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    SimplePatternLength
                  ),
                  PatternRelationship(
                    anonRel4,
                    (movie6, anonNode5),
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    SimplePatternLength
                  )
                ),
                selections = Selections.from(Set(
                  hasLabels(movie6, "Movie"),
                  differentRelationships(anonRel3, anonRel1),
                  differentRelationships(anonRel4, anonRel1),
                  differentRelationships(anonRel4, anonRel3)
                ))
              ),
              horizon = AggregatingQueryProjection(
                groupingExpressions = Map(movie6 -> movie6),
                aggregationExpressions = Map(rows11 -> collect(mapOf(movie6.name -> movie6)))
              ),
              tail = Some(
                RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(argumentIds = Set(movie6, rows11)),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map.empty,
                    aggregationExpressions = Map(
                      candidateIds8 -> compileEntityFilter(id(movie6), "moviePlots"),
                      groups9 -> collect(mapOf(movie6.name -> movie6, rows11.name -> rows11))
                    )
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(
                        patternNodes = Set(movie7),
                        argumentIds = Set(candidateIds8, groups9),
                        searchClause = Some(VectorSearchClause(
                          resultVariable = movie7,
                          indexName = "moviePlots",
                          embedding = parameter("param", CTAny),
                          where = None,
                          limit = literal(5),
                          scoreVariable = None,
                          matchFilterConfig = Some(MatchFilterConfig(
                            candidateIds = candidateIds8,
                            groupsCollection = groups9,
                            group = group10,
                            joinPredicate = equals(prop(group10, movie6.name), movie7)
                          ))
                        ))
                      ),
                      horizon = UnwindProjection(
                        variable = record12,
                        exp = prop(group10, rows11.name),
                        importedSymbolsFromLastCallSubquery = Set.empty
                      ),
                      tail = Some(RegularSinglePlannerQuery(
                        queryGraph = QueryGraph(argumentIds = Set(candidateIds8, groups9, movie7, record12)),
                        horizon = RegularQueryProjection(projections = Map(movie -> prop(record12, movie6.name)))
                      ))
                    )
                  )
                )
              )
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(movie)),
            horizon = RegularQueryProjection(
              projections = Map(v"title" -> prop(movie, "title")),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("multiple patterns sharing a variable") {
    versionsExcept5 { version =>
      val searchQuery =
        """
          |MATCH ()-->(movie:Movie)-->(), (movie)-->()
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)

      val movie = v"movie"
      val rows = v"rows"

      val anonNode0 = anonVar(0)
      val anonRel1 = anonVar(1)
      val anonRel2 = anonVar(2)
      val anonNode3 = anonVar(3)
      val anonRel4 = anonVar(4)
      val anonNode5 = anonVar(5)
      val movie6 = v"inner_movie".at(6)
      val movie7 = movie.at(7)
      val candidateIds8 = v"candidateIds".at(8)
      val groups9 = v"groups".at(9)
      val group10 = v"group".at(10)
      val rows11 = rows.at(11)
      val record12 = v"record".at(12)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          horizon = CallSubqueryHorizon(
            correlated = false,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set.empty,
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(
                patternNodes = Set(anonNode0, anonNode3, anonNode5, movie6),
                patternRelationships = Set(
                  PatternRelationship(
                    anonRel1,
                    (anonNode0, movie6),
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    SimplePatternLength
                  ),
                  PatternRelationship(
                    anonRel2,
                    (movie6, anonNode3),
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    SimplePatternLength
                  ),
                  PatternRelationship(
                    anonRel4,
                    (movie6, anonNode5),
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    SimplePatternLength
                  )
                ),
                selections = Selections.from(Set(
                  hasLabels(movie6, "Movie"),
                  differentRelationships(anonRel1, anonRel4),
                  differentRelationships(anonRel2, anonRel1),
                  differentRelationships(anonRel2, anonRel4)
                ))
              ),
              horizon = AggregatingQueryProjection(
                groupingExpressions = Map(movie6 -> movie6),
                aggregationExpressions = Map(rows11 -> collect(mapOf(movie6.name -> movie6)))
              ),
              tail = Some(
                RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(argumentIds = Set(movie6, rows11)),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map.empty,
                    aggregationExpressions = Map(
                      candidateIds8 -> compileEntityFilter(id(movie6), "moviePlots"),
                      groups9 -> collect(mapOf(movie6.name -> movie6, rows11.name -> rows11))
                    )
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(
                        patternNodes = Set(movie7),
                        argumentIds = Set(candidateIds8, groups9),
                        searchClause = Some(VectorSearchClause(
                          resultVariable = movie7,
                          indexName = "moviePlots",
                          embedding = parameter("param", CTAny),
                          where = None,
                          limit = literal(5),
                          scoreVariable = None,
                          matchFilterConfig = Some(MatchFilterConfig(
                            candidateIds = candidateIds8,
                            groupsCollection = groups9,
                            group = group10,
                            joinPredicate = equals(prop(group10, movie6.name), movie7)
                          ))
                        ))
                      ),
                      horizon = UnwindProjection(
                        variable = record12,
                        exp = prop(group10, rows11.name),
                        importedSymbolsFromLastCallSubquery = Set.empty
                      ),
                      tail = Some(RegularSinglePlannerQuery(
                        queryGraph = QueryGraph(argumentIds = Set(candidateIds8, groups9, movie7, record12)),
                        horizon = RegularQueryProjection(projections = Map(movie -> prop(record12, movie6.name)))
                      ))
                    )
                  )
                )
              )
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(movie)),
            horizon = RegularQueryProjection(
              projections = Map(v"title" -> prop(movie, "title")),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("multiple disconnected patterns") {
    versionsExcept5 { version =>
      val searchQuery =
        """
          |MATCH ()-->(movie:Movie)-->(), (a)-->(b)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)

      val a = v"a"
      val b = v"b"
      val movie = v"movie"
      val rows = v"rows"

      val anonNode0 = anonVar(0)
      val anonRel1 = anonVar(1)
      val anonRel2 = anonVar(2)
      val anonNode3 = anonVar(3)
      val a5 = v"inner_a".at(5)
      val b6 = v"inner_b".at(6)
      val movie7 = v"inner_movie".at(7)
      val anonRel4 = anonVar(4)
      val movie8 = movie.at(8)
      val candidateIds9 = v"candidateIds".at(9)
      val groups10 = v"groups".at(10)
      val group11 = v"group".at(11)
      val rows12 = rows.at(12)
      val record13 = v"record".at(13)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          horizon = CallSubqueryHorizon(
            correlated = false,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set.empty,
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(
                patternNodes = Set(anonNode0, anonNode3, a5, b6, movie7),
                patternRelationships = Set(
                  PatternRelationship(
                    anonRel1,
                    (anonNode0, movie7),
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    SimplePatternLength
                  ),
                  PatternRelationship(
                    anonRel4,
                    (a5, b6),
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    SimplePatternLength
                  ),
                  PatternRelationship(
                    anonRel2,
                    (movie7, anonNode3),
                    SemanticDirection.OUTGOING,
                    Seq.empty,
                    SimplePatternLength
                  )
                ),
                selections = Selections.from(Set(
                  hasLabels(movie7, "Movie"),
                  differentRelationships(anonRel1, anonRel4),
                  differentRelationships(anonRel2, anonRel1),
                  differentRelationships(anonRel2, anonRel4)
                ))
              ),
              horizon = AggregatingQueryProjection(
                groupingExpressions = Map(movie7 -> movie7),
                aggregationExpressions =
                  Map(rows12 -> collect(mapOf(a5.name -> a5, b6.name -> b6, movie7.name -> movie7)))
              ),
              tail = Some(
                RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(argumentIds = Set(movie7, rows12)),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map.empty,
                    aggregationExpressions = Map(
                      candidateIds9 -> compileEntityFilter(id(movie7), "moviePlots"),
                      groups10 -> collect(mapOf(movie7.name -> movie7, rows12.name -> rows12))
                    )
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(
                        patternNodes = Set(movie8),
                        argumentIds = Set(candidateIds9, groups10),
                        searchClause = Some(VectorSearchClause(
                          resultVariable = movie8,
                          indexName = "moviePlots",
                          embedding = parameter("param", CTAny),
                          where = None,
                          limit = literal(5),
                          scoreVariable = None,
                          matchFilterConfig = Some(MatchFilterConfig(
                            candidateIds = candidateIds9,
                            groupsCollection = groups10,
                            group = group11,
                            joinPredicate = equals(prop(group11, movie7.name), movie8)
                          ))
                        ))
                      ),
                      horizon = UnwindProjection(
                        variable = record13,
                        exp = prop(group11, rows12.name),
                        importedSymbolsFromLastCallSubquery = Set.empty
                      ),
                      tail = Some(RegularSinglePlannerQuery(
                        queryGraph = QueryGraph(argumentIds = Set(candidateIds9, groups10, movie8, record13)),
                        horizon = RegularQueryProjection(projections =
                          Map(
                            a -> prop(record13, a5.name),
                            b -> prop(record13, b6.name),
                            movie -> prop(record13, movie7.name)
                          )
                        )
                      ))
                    )
                  )
                )
              )
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(a, b, movie)),
            horizon = RegularQueryProjection(
              projections = Map(v"title" -> prop(movie, "title")),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("quantified path pattern") {
    versionsExcept5 { version =>
      val searchQuery =
        """
          |MATCH (start) ((x)-->(y) WHERE x.prop > y.prop){1, 4} (movie:Movie)-->()
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)

      val movie = v"movie"
      val rows = v"rows"
      val start = v"start"
      val x = v"x"
      val y = v"y"

      val anonRel1 = anonVar(1)
      val anonNode2 = anonVar(2)
      val anonRel4 = anonVar(4)
      val anonRelGroup7 = anonVar(7)

      val x3 = x.at(3)
      val y5 = y.at(5)
      val x6 = x.at(6)
      val y8 = y.at(8)

      val xGroup9 = v"inner_x".at(9)
      val yGroup10 = v"inner_y".at(10)
      val movie11 = v"inner_movie".at(11)
      val start12 = v"inner_start".at(12)
      val movie13 = movie.at(13)
      val candidateIds14 = v"candidateIds".at(14)
      val groups15 = v"groups".at(15)
      val group16 = v"group".at(16)
      val rows17 = rows.at(17)
      val record18 = v"record".at(18)

      val qpp = QuantifiedPathPattern(
        leftBinding = NodeBinding(x3, start12),
        rightBinding = NodeBinding(y5, movie11),
        patternRelationships = NonEmptyList(PatternRelationship(
          anonRel4,
          (x3, y5),
          SemanticDirection.OUTGOING,
          Seq.empty,
          SimplePatternLength
        )),
        selections = Selections.from(Set(greaterThan(prop(x3, "prop"), prop(y5, "prop")))),
        repetition = Repetition(min = 1, max = UpperBound.Limited(4)),
        nodeVariableGroupings = Set(variableGrouping(x3, xGroup9), variableGrouping(y5, yGroup10)),
        relationshipVariableGroupings = Set(variableGrouping(anonRel4, anonRelGroup7))
      )

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          horizon = CallSubqueryHorizon(
            correlated = false,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set.empty,
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(
                patternNodes = Set(anonNode2, movie11, start12),
                patternRelationships = Set(PatternRelationship(
                  anonRel1,
                  (movie11, anonNode2),
                  SemanticDirection.OUTGOING,
                  Seq.empty,
                  SimplePatternLength
                )),
                quantifiedPathPatterns = Set(qpp),
                selections = Selections.from(Set(
                  noneOfRels(anonRel1, anonRelGroup7),
                  hasLabels(movie11, "Movie"),
                  unique(anonRelGroup7)
                ))
              ),
              horizon = AggregatingQueryProjection(
                groupingExpressions = Map(movie11 -> movie11),
                aggregationExpressions = Map(rows17 -> collect(mapOf(
                  movie11.name -> movie11,
                  start12.name -> start12,
                  xGroup9.name -> xGroup9,
                  yGroup10.name -> yGroup10
                )))
              ),
              tail = Some(
                RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(argumentIds = Set(movie11, rows17)),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map.empty,
                    aggregationExpressions = Map(
                      candidateIds14 -> compileEntityFilter(id(movie11), "moviePlots"),
                      groups15 -> collect(mapOf(movie11.name -> movie11, rows17.name -> rows17))
                    )
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(
                        patternNodes = Set(movie13),
                        argumentIds = Set(candidateIds14, groups15),
                        searchClause = Some(VectorSearchClause(
                          resultVariable = movie13,
                          indexName = "moviePlots",
                          embedding = parameter("param", CTAny),
                          where = None,
                          limit = literal(5),
                          scoreVariable = None,
                          matchFilterConfig = Some(MatchFilterConfig(
                            candidateIds = candidateIds14,
                            groupsCollection = groups15,
                            group = group16,
                            joinPredicate = equals(prop(group16, movie11.name), movie13)
                          ))
                        ))
                      ),
                      horizon = UnwindProjection(
                        variable = record18,
                        exp = prop(group16, rows17.name),
                        importedSymbolsFromLastCallSubquery = Set.empty
                      ),
                      tail = Some(RegularSinglePlannerQuery(
                        queryGraph = QueryGraph(argumentIds = Set(candidateIds14, groups15, movie13, record18)),
                        horizon = RegularQueryProjection(projections =
                          Map(
                            x6 -> prop(record18, xGroup9.name),
                            y8 -> prop(record18, yGroup10.name),
                            movie -> prop(record18, movie11.name),
                            start -> prop(record18, start12.name)
                          )
                        )
                      ))
                    )
                  )
                )
              )
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(x6, y8, movie, start)),
            horizon = RegularQueryProjection(
              projections = Map(v"title" -> prop(movie, "title")),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("variable length relationship") {
    versionsExcept5 { version =>
      val searchQuery =
        """
          |MATCH (movie)-[r*1..4]->(person:Person)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)

      val movie = v"movie"
      val person = v"person"
      val r = v"r"
      val rows = v"rows"

      val movie0 = v"inner_movie".at(0)
      val person1 = v"inner_person".at(1)
      val r2 = v"inner_r".at(2)
      val movie3 = movie.at(3)
      val candidateIds4 = v"candidateIds".at(4)
      val groups5 = v"groups".at(5)
      val group6 = v"group".at(6)
      val rows7 = rows.at(7)
      val record8 = v"record".at(8)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          horizon = CallSubqueryHorizon(
            correlated = false,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set.empty,
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(
                patternNodes = Set(movie0, person1),
                patternRelationships = Set(PatternRelationship(
                  r2,
                  (movie0, person1),
                  SemanticDirection.OUTGOING,
                  Seq.empty,
                  VarPatternLength(1, Some(4))
                )),
                selections = Selections.from(Set(
                  hasLabels(person1, "Person"),
                  VarLengthLowerBound(varFor(r2.name), 1L)(pos),
                  VarLengthUpperBound(varFor(r2.name), 4L)(pos),
                  unique(r2)
                ))
              ),
              horizon = AggregatingQueryProjection(
                groupingExpressions = Map(movie0 -> movie0),
                aggregationExpressions =
                  Map(rows7 -> collect(mapOf(movie0.name -> movie0, person1.name -> person1, r2.name -> r2)))
              ),
              tail = Some(
                RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(argumentIds = Set(movie0, rows7)),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map.empty,
                    aggregationExpressions = Map(
                      candidateIds4 -> compileEntityFilter(id(movie0), "moviePlots"),
                      groups5 -> collect(mapOf(movie0.name -> movie0, rows7.name -> rows7))
                    )
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(
                        patternNodes = Set(movie3),
                        argumentIds = Set(candidateIds4, groups5),
                        searchClause = Some(VectorSearchClause(
                          resultVariable = movie3,
                          indexName = "moviePlots",
                          embedding = parameter("param", CTAny),
                          where = None,
                          limit = literal(5),
                          scoreVariable = None,
                          matchFilterConfig = Some(MatchFilterConfig(
                            candidateIds = candidateIds4,
                            groupsCollection = groups5,
                            group = group6,
                            joinPredicate = equals(prop(group6, movie0.name), movie3)
                          ))
                        ))
                      ),
                      horizon = UnwindProjection(
                        variable = record8,
                        exp = prop(group6, rows7.name),
                        importedSymbolsFromLastCallSubquery = Set.empty
                      ),
                      tail = Some(RegularSinglePlannerQuery(
                        queryGraph = QueryGraph(argumentIds = Set(candidateIds4, groups5, movie3, record8)),
                        horizon = RegularQueryProjection(projections =
                          Map(
                            movie -> prop(record8, movie0.name),
                            person -> prop(record8, person1.name),
                            r -> prop(record8, r2.name)
                          )
                        )
                      ))
                    )
                  )
                )
              )
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(movie, person, r)),
            horizon = RegularQueryProjection(
              projections = Map(v"title" -> prop(movie, "title")),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("node pattern predicate") {
    versionsExcept5 { version =>
      val searchQuery =
        """
          |MATCH (movie)-[r]->(person:Person WHERE person.name = 'Alice')
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)

      val movie = v"movie"
      val person = v"person"
      val r = v"r"
      val rows = v"rows"

      val movie0 = v"inner_movie".at(0)
      val person1 = v"inner_person".at(1)
      val r2 = v"inner_r".at(2)
      val movie3 = movie.at(3)
      val candidateIds4 = v"candidateIds".at(4)
      val groups5 = v"groups".at(5)
      val group6 = v"group".at(6)
      val rows7 = rows.at(7)
      val record8 = v"record".at(8)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          horizon = CallSubqueryHorizon(
            correlated = false,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set.empty,
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(
                patternNodes = Set(movie0, person1),
                patternRelationships = Set(PatternRelationship(
                  r2,
                  (movie0, person1),
                  SemanticDirection.OUTGOING,
                  Seq.empty,
                  SimplePatternLength
                )),
                selections = Selections.from(Set(
                  in(prop(person1, "name"), listOf(literal("Alice"))),
                  hasLabels(person1, "Person")
                ))
              ),
              horizon = AggregatingQueryProjection(
                groupingExpressions = Map(movie0 -> movie0),
                aggregationExpressions =
                  Map(rows7 -> collect(mapOf(movie0.name -> movie0, person1.name -> person1, r2.name -> r2)))
              ),
              tail = Some(
                RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(argumentIds = Set(movie0, rows7)),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map.empty,
                    aggregationExpressions = Map(
                      candidateIds4 -> compileEntityFilter(id(movie0), "moviePlots"),
                      groups5 -> collect(mapOf(movie0.name -> movie0, rows7.name -> rows7))
                    )
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(
                        patternNodes = Set(movie3),
                        argumentIds = Set(candidateIds4, groups5),
                        searchClause = Some(VectorSearchClause(
                          resultVariable = movie3,
                          indexName = "moviePlots",
                          embedding = parameter("param", CTAny),
                          where = None,
                          limit = literal(5),
                          scoreVariable = None,
                          matchFilterConfig = Some(MatchFilterConfig(
                            candidateIds = candidateIds4,
                            groupsCollection = groups5,
                            group = group6,
                            joinPredicate = equals(prop(group6, movie0.name), movie3)
                          ))
                        ))
                      ),
                      horizon = UnwindProjection(
                        variable = record8,
                        exp = prop(group6, rows7.name),
                        importedSymbolsFromLastCallSubquery = Set.empty
                      ),
                      tail = Some(RegularSinglePlannerQuery(
                        queryGraph = QueryGraph(argumentIds = Set(candidateIds4, groups5, movie3, record8)),
                        horizon = RegularQueryProjection(projections =
                          Map(
                            movie -> prop(record8, movie0.name),
                            person -> prop(record8, person1.name),
                            r -> prop(record8, r2.name)
                          )
                        )
                      ))
                    )
                  )
                )
              )
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(movie, person, r)),
            horizon = RegularQueryProjection(
              projections = Map(v"title" -> prop(movie, "title")),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("should propagate prior bound symbols into subquery correctly") {
    versionsExcept5 { version =>
      val query =
        """
          |MATCH (hello)
          |MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR hello.embedding
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(query, version)

      val hello = v"hello"
      val director = v"director"
      val movie = v"movie"
      val rel = v"rel"
      val rows = v"rows"

      val director0 = v"inner_director".at(0)
      val movie1 = v"inner_movie".at(1)
      val rel2 = v"inner_rel".at(2)
      val movie3 = movie.at(3)
      val candidateIds4 = v"candidateIds".at(4)
      val groups5 = v"groups".at(5)
      val group6 = v"group".at(6)
      val rows7 = rows.at(7)
      val record8 = v"record".at(8)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          queryGraph = QueryGraph(patternNodes = Set(hello)),
          horizon = PassthroughAllHorizon(Set.empty),
          tail = Some(
            RegularSinglePlannerQuery(
              queryGraph = QueryGraph(argumentIds = Set(hello)),
              horizon = CallSubqueryHorizon(
                correlated = true,
                yielding = true,
                inTransactionsParameters = None,
                optional = false,
                importedVariables = Set(hello),
                importedSymbolsFromLastCallSubquery = Set.empty,
                callSubquery = RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(
                    patternNodes = Set(director0, movie1),
                    patternRelationships = Set(PatternRelationship(
                      rel2,
                      (movie1, director0),
                      SemanticDirection.INCOMING,
                      Seq(relTypeName("DIRECTED")),
                      SimplePatternLength
                    )),
                    argumentIds = Set(hello),
                    selections = Selections.from(Set(hasLabels(director0, "Person"), hasLabels(movie1, "Movie")))
                  ),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map(movie1 -> movie1),
                    aggregationExpressions = Map(rows7 -> collect(mapOf(
                      director0.name -> director0,
                      movie1.name -> movie1,
                      rel2.name -> rel2
                    ))),
                    importedExposedSymbols = Set(hello)
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(argumentIds = Set(movie1, rows7, hello)),
                      horizon = AggregatingQueryProjection(
                        groupingExpressions = Map.empty,
                        aggregationExpressions = Map(
                          candidateIds4 -> compileEntityFilter(id(movie1), "moviePlots"),
                          groups5 -> collect(mapOf(movie1.name -> movie1, rows7.name -> rows7))
                        ),
                        importedExposedSymbols = Set(hello)
                      ),
                      tail = Some(
                        RegularSinglePlannerQuery(
                          queryGraph = QueryGraph(
                            patternNodes = Set(movie3),
                            argumentIds = Set(candidateIds4, groups5, hello),
                            searchClause = Some(VectorSearchClause(
                              resultVariable = movie3,
                              indexName = "moviePlots",
                              embedding = prop(hello, "embedding"),
                              where = None,
                              limit = literal(5),
                              scoreVariable = None,
                              matchFilterConfig = Some(MatchFilterConfig(
                                candidateIds = candidateIds4,
                                groupsCollection = groups5,
                                group = group6,
                                joinPredicate = equals(prop(group6, movie1.name), movie3)
                              ))
                            ))
                          ),
                          horizon = UnwindProjection(
                            variable = record8,
                            exp = prop(group6, rows7.name),
                            importedSymbolsFromLastCallSubquery = Set(hello)
                          ),
                          tail = Some(RegularSinglePlannerQuery(
                            queryGraph = QueryGraph(argumentIds = Set(candidateIds4, groups5, movie3, record8, hello)),
                            horizon = RegularQueryProjection(
                              projections = Map(
                                director -> prop(record8, director0.name),
                                movie -> prop(record8, movie1.name),
                                rel -> prop(record8, rel2.name)
                              ),
                              importedExposedSymbols = Set(hello)
                            )
                          ))
                        )
                      )
                    )
                  )
                )
              ),
              tail = Some(RegularSinglePlannerQuery(
                queryGraph = QueryGraph(argumentIds = Set(director, hello, movie, rel)),
                horizon = RegularQueryProjection(
                  projections = Map(v"title" -> prop(movie, "title")),
                  position = QueryProjection.Position.Final
                )
              ))
            )
          )
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("should group by pattern elements that the SEARCH depends on") {
    versionsExcept5 { version =>
      val query =
        """
          |MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR director.embedding
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(query, version)

      val director = v"director"
      val movie = v"movie"
      val rel = v"rel"
      val rows = v"rows"

      val director0 = v"inner_director".at(0)
      val movie1 = v"inner_movie".at(1)
      val rel2 = v"inner_rel".at(2)
      val movie3 = movie.at(3)
      val candidateIds4 = v"candidateIds".at(4)
      val groups5 = v"groups".at(5)
      val group6 = v"group".at(6)
      val rows7 = rows.at(7)
      val record8 = v"record".at(8)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          horizon = CallSubqueryHorizon(
            correlated = false,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set.empty,
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(
                patternNodes = Set(director0, movie1),
                patternRelationships = Set(PatternRelationship(
                  rel2,
                  (movie1, director0),
                  SemanticDirection.INCOMING,
                  Seq(relTypeName("DIRECTED")),
                  SimplePatternLength
                )),
                selections = Selections.from(Set(hasLabels(director0, "Person"), hasLabels(movie1, "Movie")))
              ),
              horizon = AggregatingQueryProjection(
                groupingExpressions = Map(director0 -> director0, movie1 -> movie1),
                aggregationExpressions =
                  Map(rows7 -> collect(mapOf(director0.name -> director0, movie1.name -> movie1, rel2.name -> rel2)))
              ),
              tail = Some(
                RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(argumentIds = Set(director0, movie1, rows7)),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map(director0 -> director0),
                    aggregationExpressions = Map(
                      candidateIds4 -> compileEntityFilter(id(movie1), "moviePlots"),
                      groups5 -> collect(mapOf(movie1.name -> movie1, rows7.name -> rows7))
                    )
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(
                        patternNodes = Set(movie3),
                        argumentIds = Set(candidateIds4, groups5, director0),
                        searchClause = Some(VectorSearchClause(
                          resultVariable = movie3,
                          indexName = "moviePlots",
                          embedding = prop(director0, "embedding"),
                          where = None,
                          limit = literal(5),
                          scoreVariable = None,
                          matchFilterConfig = Some(MatchFilterConfig(
                            candidateIds = candidateIds4,
                            groupsCollection = groups5,
                            group = group6,
                            joinPredicate = equals(prop(group6, movie1.name), movie3)
                          ))
                        ))
                      ),
                      horizon = UnwindProjection(
                        variable = record8,
                        exp = prop(group6, rows7.name),
                        importedSymbolsFromLastCallSubquery = Set.empty
                      ),
                      tail = Some(RegularSinglePlannerQuery(
                        queryGraph = QueryGraph(argumentIds = Set(candidateIds4, groups5, director0, movie3, record8)),
                        horizon = RegularQueryProjection(projections =
                          Map(
                            director -> prop(record8, director0.name),
                            movie -> prop(record8, movie1.name),
                            rel -> prop(record8, rel2.name)
                          )
                        )
                      ))
                    )
                  )
                )
              )
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(director, movie, rel)),
            horizon = RegularQueryProjection(
              projections = Map(v"title" -> prop(movie, "title")),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("predicates combining the score variable and a pattern variable are applied as a post-filter") {
    versionsExcept5 { version =>
      val query =
        """
          |MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR director.embedding
          |    LIMIT 5
          |  ) SCORE AS score
          |WHERE score > rel.prop
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(query, version)

      val director = v"director"
      val movie = v"movie"
      val rel = v"rel"
      val rows = v"rows"

      val director0 = v"inner_director".at(0)
      val movie1 = v"inner_movie".at(1)
      val rel2 = v"inner_rel".at(2)
      val movie3 = movie.at(3)
      val candidateIds4 = v"candidateIds".at(4)
      val groups5 = v"groups".at(5)
      val group6 = v"group".at(6)
      val rows7 = rows.at(7)
      val record8 = v"record".at(8)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          horizon = CallSubqueryHorizon(
            correlated = false,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set.empty,
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(
                patternNodes = Set(director0, movie1),
                patternRelationships = Set(PatternRelationship(
                  rel2,
                  (movie1, director0),
                  SemanticDirection.INCOMING,
                  Seq(relTypeName("DIRECTED")),
                  SimplePatternLength
                )),
                selections = Selections.from(Set(hasLabels(director0, "Person"), hasLabels(movie1, "Movie")))
              ),
              horizon = AggregatingQueryProjection(
                groupingExpressions = Map(director0 -> director0, movie1 -> movie1),
                aggregationExpressions =
                  Map(rows7 -> collect(mapOf(director0.name -> director0, movie1.name -> movie1, rel2.name -> rel2)))
              ),
              tail = Some(
                RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(argumentIds = Set(director0, movie1, rows7)),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map(director0 -> director0),
                    aggregationExpressions = Map(
                      candidateIds4 -> compileEntityFilter(id(movie1), "moviePlots"),
                      groups5 -> collect(mapOf(movie1.name -> movie1, rows7.name -> rows7))
                    )
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(
                        patternNodes = Set(movie3),
                        argumentIds = Set(candidateIds4, groups5, director0),
                        searchClause = Some(VectorSearchClause(
                          resultVariable = movie3,
                          indexName = "moviePlots",
                          embedding = prop(director0, "embedding"),
                          where = None,
                          limit = literal(5),
                          scoreVariable = Some(v"score"),
                          matchFilterConfig = Some(MatchFilterConfig(
                            candidateIds = candidateIds4,
                            groupsCollection = groups5,
                            group = group6,
                            joinPredicate = equals(prop(group6, movie1.name), movie3)
                          ))
                        ))
                      ),
                      horizon = UnwindProjection(
                        variable = record8,
                        exp = prop(group6, rows7.name),
                        importedSymbolsFromLastCallSubquery = Set.empty
                      ),
                      tail = Some(RegularSinglePlannerQuery(
                        queryGraph = QueryGraph(argumentIds =
                          Set(candidateIds4, groups5, director0, movie3, record8, v"score")
                        ),
                        horizon = RegularQueryProjection(
                          projections = Map(
                            director -> prop(record8, director0.name),
                            movie -> prop(record8, movie1.name),
                            rel -> prop(record8, rel2.name),
                            v"score" -> v"score"
                          ),
                          selections = Selections.from(Set(lessThan(prop(rel, "prop"), v"score")))
                        )
                      ))
                    )
                  )
                )
              )
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(director, movie, rel, v"score")),
            horizon = RegularQueryProjection(
              projections = Map(v"title" -> prop(movie, "title")),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("should propagate symbols imported into CALL") {
    versionsExcept5 { version =>
      val query =
        """MATCH (hello)-->(world)
          |CALL (world) {
          |  MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
          |    SEARCH movie IN (
          |      VECTOR INDEX moviePlots
          |      FOR world.embedding
          |      LIMIT 5
          |    )
          |  RETURN movie.title AS title
          |}
          |RETURN hello, world, title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(query, version)

      val hello = v"hello"
      val world = v"world"
      val director = v"director"
      val movie = v"movie"
      val rel = v"rel"
      val rows = v"rows"
      val title = v"title"

      val anonRel0 = anonVar(0)
      val director1 = v"inner_director".at(1)
      val movie2 = v"inner_movie".at(2)
      val rel3 = v"inner_rel".at(3)
      val movie4 = movie.at(4)
      val candidateIds5 = v"candidateIds".at(5)
      val groups6 = v"groups".at(6)
      val group7 = v"group".at(7)
      val rows8 = rows.at(8)
      val record9 = v"record".at(9)

      searchPlannerQuery shouldEqual
        RegularSinglePlannerQuery(
          queryGraph = QueryGraph(
            patternNodes = Set(hello, world),
            patternRelationships =
              Set(PatternRelationship(
                anonRel0,
                (hello, world),
                SemanticDirection.OUTGOING,
                Seq.empty,
                SimplePatternLength
              ))
          ),
          horizon = CallSubqueryHorizon(
            correlated = true,
            yielding = true,
            inTransactionsParameters = None,
            optional = false,
            importedVariables = Set(world),
            importedSymbolsFromLastCallSubquery = Set.empty,
            callSubquery = RegularSinglePlannerQuery(
              queryGraph = QueryGraph(argumentIds = Set(world)),
              horizon = CallSubqueryHorizon(
                correlated = true,
                yielding = true,
                inTransactionsParameters = None,
                optional = false,
                importedVariables = Set(world),
                importedSymbolsFromLastCallSubquery = Set(world),
                callSubquery = RegularSinglePlannerQuery(
                  queryGraph = QueryGraph(
                    patternNodes = Set(director1, movie2),
                    patternRelationships = Set(PatternRelationship(
                      rel3,
                      (movie2, director1),
                      SemanticDirection.INCOMING,
                      Seq(relTypeName("DIRECTED")),
                      SimplePatternLength
                    )),
                    argumentIds = Set(world),
                    selections = Selections.from(Set(hasLabels(director1, "Person"), hasLabels(movie2, "Movie")))
                  ),
                  horizon = AggregatingQueryProjection(
                    groupingExpressions = Map(movie2 -> movie2),
                    aggregationExpressions = Map(rows8 -> collect(mapOf(
                      director1.name -> director1,
                      movie2.name -> movie2,
                      rel3.name -> rel3
                    ))),
                    importedExposedSymbols = Set(world)
                  ),
                  tail = Some(
                    RegularSinglePlannerQuery(
                      queryGraph = QueryGraph(argumentIds = Set(movie2, rows8, world)),
                      horizon = AggregatingQueryProjection(
                        groupingExpressions = Map.empty,
                        aggregationExpressions = Map(
                          candidateIds5 -> compileEntityFilter(id(movie2), "moviePlots"),
                          groups6 -> collect(mapOf(movie2.name -> movie2, rows8.name -> rows8))
                        ),
                        importedExposedSymbols = Set(world)
                      ),
                      tail = Some(
                        RegularSinglePlannerQuery(
                          queryGraph = QueryGraph(
                            patternNodes = Set(movie4),
                            argumentIds = Set(candidateIds5, groups6, world),
                            searchClause = Some(VectorSearchClause(
                              resultVariable = movie4,
                              indexName = "moviePlots",
                              embedding = prop(world, "embedding"),
                              where = None,
                              limit = literal(5),
                              scoreVariable = None,
                              matchFilterConfig = Some(MatchFilterConfig(
                                candidateIds = candidateIds5,
                                groupsCollection = groups6,
                                group = group7,
                                joinPredicate = equals(prop(group7, movie2.name), movie4)
                              ))
                            ))
                          ),
                          horizon = UnwindProjection(
                            variable = record9,
                            exp = prop(group7, rows8.name),
                            importedSymbolsFromLastCallSubquery = Set(world)
                          ),
                          tail = Some(RegularSinglePlannerQuery(
                            queryGraph = QueryGraph(argumentIds = Set(candidateIds5, groups6, movie4, record9, world)),
                            horizon = RegularQueryProjection(
                              projections = Map(
                                director -> prop(record9, director1.name),
                                movie -> prop(record9, movie2.name),
                                rel -> prop(record9, rel3.name)
                              ),
                              importedExposedSymbols = Set(world)
                            )
                          ))
                        )
                      )
                    )
                  )
                )
              ),
              tail = Some(RegularSinglePlannerQuery(
                queryGraph = QueryGraph(argumentIds = Set(director, movie, rel, world)),
                horizon = RegularQueryProjection(
                  projections = Map(title -> prop(movie, "title")),
                  importedExposedSymbols = Set(world)
                )
              ))
            )
          ),
          tail = Some(RegularSinglePlannerQuery(
            queryGraph = QueryGraph(argumentIds = Set(anonRel0, hello, title, world)),
            horizon = RegularQueryProjection(
              projections = Map(hello -> hello, world -> world, title -> title),
              position = QueryProjection.Position.Final
            )
          ))
        )

      assertCanExtractMatchFilterSubquery(searchPlannerQuery)
    }
  }

  test("should mark subquery as OPTIONAL") {
    versionsExcept5 { version =>
      val searchQuery =
        """
          |OPTIONAL MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)
      inside(searchPlannerQuery) {
        case RegularSinglePlannerQuery(_, _, horizon: CallSubqueryHorizon, _, _) =>
          horizon.optional shouldBe true
          SearchWithComplexPatternConverter.MatchFilterSubquery.unapply(horizon) shouldBe defined
      }
    }
  }

  test("should extract match pattern query graph with SEARCH clause included") {
    versionsExcept5 { version =>
      val searchQuery =
        """
          |MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
          |  SEARCH movie IN (
          |    VECTOR INDEX moviePlots
          |    FOR $param
          |    WHERE movie.imdbRating > director.prop
          |    LIMIT 5
          |  )
          |RETURN movie.title AS title
          |""".stripMargin

      val searchPlannerQuery = buildSinglePlannerQuery(searchQuery, version)

      val director0 = v"inner_director".at(0)
      val movie1 = v"inner_movie".at(1)
      val rel2 = v"inner_rel".at(2)

      inside(searchPlannerQuery.horizon) {
        case SearchWithComplexPatternConverter.MatchFilterSubquery(matchFilterQueryGraph) =>
          matchFilterQueryGraph shouldEqual
            QueryGraph(
              patternNodes = Set(director0, movie1),
              patternRelationships = Set(PatternRelationship(
                rel2,
                (movie1, director0),
                SemanticDirection.INCOMING,
                Seq(relTypeName("DIRECTED")),
                SimplePatternLength
              )),
              selections = Selections.from(Set(hasLabels(director0, "Person"), hasLabels(movie1, "Movie"))),
              searchClause = Some(VectorSearchClause(
                resultVariable = movie1,
                indexName = "moviePlots",
                embedding = parameter("param", CTAny),
                where = Some(where(greaterThan(prop(movie1, "imdbRating"), prop(director0, "prop")))),
                limit = literal(5),
                scoreVariable = None,
                matchFilterConfig = None
              ))
            )
      }
    }
  }
}
