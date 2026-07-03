/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.CollectSyntaxUsageMetrics
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CollectSyntaxUsageMetricsTest extends CypherFunSuite with CypherVersionTestSupport {

  private def pipeline =
    Parse andThen CollectSyntaxUsageMetrics

  testVersions("should find multiple things in one query") { version =>
    val stats = runPipeline(
      version,
      """
        |MATCH ANY SHORTEST (a)-->*(b)
        |MATCH ANY SHORTEST (c)-->*(d)
        |WITH shortestPath( (a)-[*]->(d) ) AS p
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GPM_SHORTEST) should be(2)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LEGACY_SHORTEST) should be(1)
  }

  testVersions("should find GPM SHORTEST") { version =>
    val stats = runPipeline(
      version,
      """
        |MATCH ANY SHORTEST (a)-->*(b)
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GPM_SHORTEST) should be(1)
  }

  testVersions("should find LEGACY SHORTEST in MATCH") { version =>
    val stats = runPipeline(
      version,
      """
        |MATCH shortestPath( (a)-[*]->(b) )
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LEGACY_SHORTEST) should be(1)
  }

  testVersions("should find LEGACY SHORTEST in WITH") { version =>
    val stats = runPipeline(
      version,
      """
        |MATCH (a), (b) WITH shortestPath( (a)-[*]->(b) ) AS p
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LEGACY_SHORTEST) should be(1)
  }

  testVersions("should find COLLECT subquery") { version =>
    val stats = runPipeline(
      version,
      """
        |RETURN COLLECT { MATCH (a) RETURN a } AS as
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.COLLECT_SUBQUERY) should be(1)
  }

  testVersions("should find COUNT subquery") { version =>
    val stats = runPipeline(
      version,
      """
        |RETURN COUNT { MATCH (a) RETURN a } AS as
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.COUNT_SUBQUERY) should be(1)
  }

  testVersions("should find EXISTS subquery") { version =>
    val stats = runPipeline(
      version,
      """
        |RETURN EXISTS { MATCH (a) RETURN a } AS as
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.EXISTS_SUBQUERY) should be(1)
  }

  testVersions("should find QPP") { version =>
    val stats = runPipeline(
      version,
      """
        |MATCH ANY SHORTEST (a)( ()-->() )*(b)
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.QUANTIFIED_PATH_PATTERN) should be(1)
  }

  testVersions("should find QPP syntactic sugar") { version =>
    val stats = runPipeline(
      version,
      """
        |MATCH ANY SHORTEST (a)-->*(b)
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.QUANTIFIED_PATH_PATTERN) should be(1)
  }

  testVersions("should find Repeatable Elements") { version =>
    val stats = runPipeline(
      version,
      """
        |MATCH REPEATABLE ELEMENTS (a)( ()-->() )*(b)
        |MATCH REPEATABLE ELEMENTS ANY SHORTEST (a)( ()-->() )*(b)
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.REPEATABLE_ELEMENTS) should be(2)
  }

  testVersionsExcept5("should find ACYCLIC Path Mode") { version =>
    val stats = runPipeline(
      version,
      query =
        """
          |MATCH ACYCLIC (a)-[r]-(b)((c)-[s]-(d))+ (p)
          |RETURN *
          |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ACYCLIC_PATH_MODE) should be(1)
  }

  test("should find SHORTEST + explicit path mode") {
    Seq("ACYCLIC", "TRAIL", "WALK").foreach {
      explicitPathMode =>
        withClue(
          s"SHORTEST + $explicitPathMode should increase GPM_SHORTEST and GPM_SHORTEST_WITH_EXPLICIT_PATH_MODE metrics"
        ) {
          val stats = runPipeline(
            CypherVersion.Cypher25,
            query =
              s"""
                 |MATCH ANY SHORTEST $explicitPathMode (a)-[r]-(b)((c)-[s]-(d))+ (p)
                 |RETURN *
                 |""".stripMargin
          )
          stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GPM_SHORTEST) should be(1)
          stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GPM_SHORTEST_WITH_EXPLICIT_PATH_MODE) should be(1)
          stats.getSyntaxUsageCount(SyntaxUsageMetricKey.QUANTIFIED_PATH_PATTERN) should be(1)
          if (explicitPathMode == "ACYCLIC") {
            stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ACYCLIC_PATH_MODE) should be(1)
          } else {
            stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ACYCLIC_PATH_MODE) should be(0)
          }
        }

        withClue(
          s"ALL + $explicitPathMode should not increase GPM_SHORTEST and GPM_SHORTEST_WITH_EXPLICIT_PATH_MODE metrics"
        ) {
          val stats = runPipeline(
            CypherVersion.Cypher25,
            query =
              s"""
                 |MATCH ALL $explicitPathMode (a)-[r]-(b)((c)-[s]-(d))+ (p)
                 |RETURN *
                 |""".stripMargin
          )
          stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GPM_SHORTEST) should be(0)
          stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GPM_SHORTEST_WITH_EXPLICIT_PATH_MODE) should be(0)
          stats.getSyntaxUsageCount(SyntaxUsageMetricKey.QUANTIFIED_PATH_PATTERN) should be(1)
          if (explicitPathMode == "ACYCLIC") {
            stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ACYCLIC_PATH_MODE) should be(1)
          } else {
            stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ACYCLIC_PATH_MODE) should be(0)
          }
        }
    }
  }

  testVersionsExcept5("should find LET clause") { version =>
    val stats = runPipeline(
      version,
      """
        |LET x = 1
        |RETURN x
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LET_CLAUSE) should be(1)
  }

  testVersionsExcept5("should find FILTER clause") { version =>
    val stats = runPipeline(
      version,
      """
        |LET x = 1
        |FILTER x > 0
        |RETURN x
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LET_CLAUSE) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.FILTER_CLAUSE) should be(1)
  }

  testVersionsExcept5("should find Importing with correlated") { version =>
    val stats = runPipeline(
      version,
      """
        |
        |LET x = 1
        |CALL {
        |   WITH x
        |   RETURN x + 1 AS y
        |}
        |RETURN y
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LET_CLAUSE) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY_CORRELATED) should be(1)
  }

  testVersionsExcept5("should find Importing with uncorrelated") { version =>
    val stats = runPipeline(
      version,
      """
        |
        |LET x = 1
        |CALL {
        |   RETURN 1 AS y
        |}
        |RETURN y
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LET_CLAUSE) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY_UNCORRELATED) should be(1)
  }

  testVersionsExcept5("should find Scope clause") { version =>
    val stats = runPipeline(
      version,
      """
        |LET x = 1
        |CALL (x) {
        |   RETURN x + 1 AS y
        |}
        |RETURN y
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LET_CLAUSE) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY) should be(1)
  }

  testVersionsExcept5("should find GROUP BY with explicit grouping elements") { version =>
    val stats = runPipeline(
      version,
      """
        |UNWIND [{a: 1, b: 2}] AS row
        |WITH row.a AS a, row.b AS b
        |RETURN a, count(*) AS cnt
        |  GROUP BY a
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY_EXPLICIT) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY_ALL) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY_NONE) should be(0)
  }

  testVersionsExcept5("should find GROUP BY ALL") { version =>
    val stats = runPipeline(
      version,
      """
        |UNWIND [{a: 1, b: 2}] AS row
        |WITH row.a AS a, row.b AS b
        |RETURN a, b, count(*) AS cnt
        |  GROUP BY ALL
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY_ALL) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY_EXPLICIT) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY_NONE) should be(0)
  }

  testVersionsExcept5("should find GROUP BY ()") { version =>
    val stats = runPipeline(
      version,
      """
        |UNWIND [{a: 1}] AS row
        |WITH row.a AS a
        |RETURN count(*) AS cnt
        |  GROUP BY ()
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY_NONE) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY_EXPLICIT) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GROUP_BY_ALL) should be(0)
  }

  testVersionsExcept5("should find Conditional Query") { version =>
    val stats = runPipeline(
      version,
      """
        |WHEN true THEN RETURN 1 AS y
        |WHEN false THEN RETURN 2 AS y
        |ELSE RETURN 3 AS y
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.CONDITIONAL_QUERY) should be(1)
  }

  testVersionsExcept5("should find NEXT statement") { version =>
    val stats = runPipeline(
      version,
      """
        |RETURN 1 AS x
        |
        |NEXT
        |
        |RETURN x + 1 AS y
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.NEXT_STATEMENT) should be(1)
  }

  testVersionsExcept5("should find SEARCH clause without filters") { version =>
    val stats = runPipeline(
      version,
      """
        |MATCH (movie)
        |  WHERE movie.rating > 7
        |  SEARCH movie IN (
        |    VECTOR INDEX idx
        |    FOR $embedding
        |    LIMIT 5
        |  )
        |RETURN movie.title AS title
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SEARCH_WITHOUT_FILTERS) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SEARCH_WITH_FILTERS) should be(0)
  }

  testVersionsExcept5("should find SEARCH clause with filters") { version =>
    val stats = runPipeline(
      version,
      """
        |MATCH (movie)
        |  WHERE movie.rating > 7
        |  SEARCH movie IN (
        |    VECTOR INDEX idx
        |    FOR $embedding
        |    WHERE movie.rating > 8
        |    LIMIT 5
        |  )
        |RETURN movie.title AS title
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SEARCH_WITHOUT_FILTERS) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SEARCH_WITH_FILTERS) should be(1)
  }

  testVersions("should find LOAD CSV") { version =>
    val stats = runPipeline(
      version,
      """
        |LOAD CSV WITH HEADERS FROM 'https://data.neo4j.com/importing-cypher/persons.csv' AS row
        |MERGE (p:Person {tmdbId: row.person_tmdbId})
        |SET p.name = row.name, p.born = row.born
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX_CONCURRENT) should be(0)

    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY) should be(0)
  }

  testVersions("should find LOAD CSV with CALL - importing WITH") { version =>
    val stats = runPipeline(
      version,
      """
        |LOAD CSV WITH HEADERS FROM 'https://data.neo4j.com/importing-cypher/persons.csv' AS row
        |CALL {
        |  WITH row
        |  MERGE (p:Person {tmdbId: row.person_tmdbId})
        |  SET p.name = row.name, p.born = row.born
        |}
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX_CONCURRENT) should be(0)

    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY) should be(0)
  }

  testVersions("should find LOAD CSV with CALL - scope clause") { version =>
    val stats = runPipeline(
      version,
      """
        |LOAD CSV WITH HEADERS FROM 'https://data.neo4j.com/importing-cypher/persons.csv' AS row
        |CALL (row) {
        |  MERGE (p:Person {tmdbId: row.person_tmdbId})
        |  SET p.name = row.name, p.born = row.born
        |}
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX_CONCURRENT) should be(0)

    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY) should be(1)
  }

  testVersions("should find LOAD CSV with CALL IN TX - importing WITH") { version =>
    val stats = runPipeline(
      version,
      """
        |LOAD CSV WITH HEADERS FROM 'https://data.neo4j.com/importing-cypher/persons.csv' AS row
        |CALL {
        |  WITH row
        |  MERGE (p:Person {tmdbId: row.person_tmdbId})
        |  SET p.name = row.name, p.born = row.born
        |} IN TRANSACTIONS OF 200 ROWS
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX_CONCURRENT) should be(0)

    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY) should be(0)
  }

  testVersions("should find LOAD CSV with CALL IN TX - scope clause") { version =>
    val stats = runPipeline(
      version,
      """
        |LOAD CSV WITH HEADERS FROM 'https://data.neo4j.com/importing-cypher/persons.csv' AS row
        |CALL (row) {
        |  MERGE (p:Person {tmdbId: row.person_tmdbId})
        |  SET p.name = row.name, p.born = row.born
        |} IN TRANSACTIONS OF 200 ROWS
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX_CONCURRENT) should be(0)

    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY) should be(1)
  }

  testVersions("should find LOAD CSV with CALL IN CONCURRENT TX - importing WITH") { version =>
    val stats = runPipeline(
      version,
      """
        |LOAD CSV WITH HEADERS FROM 'https://data.neo4j.com/importing-cypher/persons.csv' AS row
        |CALL {
        |  WITH row
        |  MERGE (p:Person {tmdbId: row.person_tmdbId})
        |  SET p.name = row.name, p.born = row.born
        |} IN CONCURRENT TRANSACTIONS OF 200 ROWS
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX_CONCURRENT) should be(1)

    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY) should be(0)
  }

  testVersions("should find LOAD CSV with CALL IN CONCURRENT TX - scope clause") { version =>
    val stats = runPipeline(
      version,
      """
        |LOAD CSV WITH HEADERS FROM 'https://data.neo4j.com/importing-cypher/persons.csv' AS row
        |CALL (row) {
        |  MERGE (p:Person {tmdbId: row.person_tmdbId})
        |  SET p.name = row.name, p.born = row.born
        |} IN CONCURRENT TRANSACTIONS OF 200 ROWS
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX) should be(1)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX_CONCURRENT) should be(1)

    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY) should be(1)
  }

  testVersions("should not find CALL IN TX without LOAD CSV") { version =>
    val stats = runPipeline(
      version,
      """
        | WITH {person_tmbid: 5, name:'Alice', born:date('2012')} AS row
        |CALL (row) {
        |  MERGE (p:Person {tmdbId: row.person_tmdbId})
        |  SET p.name = row.name, p.born = row.born
        |} IN TRANSACTIONS OF 200 ROWS
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX_CONCURRENT) should be(0)

    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY) should be(0)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY) should be(1)
  }

  testVersionsExcept5("should find ALTER CURRENT GRAPH TYPE") { version =>
    val stats = runPipeline(
      version,
      "ALTER CURRENT GRAPH TYPE SET { }"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ALTER_CURRENT_GRAPH_TYPE) should be(1)
  }

  testVersionsExcept5("should find SHOW CURRENT GRAPH TYPE") { version =>
    val stats = runPipeline(
      version,
      "SHOW CURRENT GRAPH TYPE"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SHOW_CURRENT_GRAPH_TYPE) should be(1)
  }

  testVersionsExcept5("should find CREATE AUTH RULE command") { version =>
    val stats = runPipeline(
      version,
      "CREATE AUTH RULE rule SET CONDITION abac.oidc.user_attribute('country') = 'SE'"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.CREATE_AUTH_RULE) should be(1)
  }

  testVersionsExcept5("should find RENAME AUTH RULE command") { version =>
    val stats = runPipeline(
      version,
      "RENAME AUTH RULE authRule TO authRule2"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.RENAME_AUTH_RULE) should be(1)
  }

  testVersionsExcept5("should find ALTER AUTH RULE command") { version =>
    val stats = runPipeline(
      version,
      "ALTER AUTH RULE rule SET CONDITION abac.oidc.user_attribute('country') = 'SE'"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ALTER_AUTH_RULE) should be(1)
  }

  testVersionsExcept5("should find DROP AUTH RULE command") { version =>
    val stats = runPipeline(
      version,
      "DROP AUTH RULE rule"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.DROP_AUTH_RULE) should be(1)
  }

  testVersionsExcept5("should find SHOW AUTH RULE command") { version =>
    val stats = runPipeline(
      version,
      "SHOW AUTH RULES"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.SHOW_AUTH_RULES) should be(1)
  }

  testVersionsExcept5("should find CREATE USER with tags") { version =>
    val stats = runPipeline(
      version,
      "CREATE USER foo SET PASSWORD 'password' SET TAGS 'label'"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.CREATE_USER_TAGS) should be(1)
  }

  testVersionsExcept5("should not find CREATE USER with tags when no tags are set") { version =>
    val stats = runPipeline(
      version,
      "CREATE USER foo SET PASSWORD 'password'"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.CREATE_USER_TAGS) should be(0)
  }

  testVersionsExcept5("should find ALTER USER with tags") { version =>
    val stats = runPipeline(
      version,
      "ALTER USER foo ADD TAGS 'tag'"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ALTER_USER_TAGS) should be(1)
  }

  testVersionsExcept5("should not find ALTER USER with tags when no tags are altered") { version =>
    val stats = runPipeline(
      version,
      "ALTER USER foo SET PASSWORD 'password'"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ALTER_USER_TAGS) should be(0)
  }

  testVersionsExcept5("should find ALTER USERS with tags") { version =>
    val stats = runPipeline(
      version,
      "ALTER USERS alice, bob ADD TAGS 'x'"
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.ALTER_USERS_TAGS) should be(1)
  }

  private def runPipeline(
    version: CypherVersion,
    query: String,
    semanticFeatures: Seq[SemanticFeature] = Seq.empty
  ): InternalUsageStats = {
    val startState = InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)
    val semanticFeaturesParam = semanticFeatures
    val context = new ErrorCollectingContext(version, query = query, semanticFeatures = semanticFeaturesParam) {
      override val internalUsageStats: InternalUsageStats = InternalUsageStats.newImpl()
    }
    pipeline.transform(startState, context)

    context.internalUsageStats
  }
}
