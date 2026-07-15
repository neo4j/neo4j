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
package org.neo4j.cypher.internal.frontend.phases.rewriting

import org.neo4j.cypher.internal.CypherVersion.Cypher25
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.SearchPredicateNormalizer
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SearchPredicateNormalizerTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should rewrite < so that binding variable is on LHS") {
    shouldRewrite(
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE m.rating < movie.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE movie.rating > m.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite <= so that binding variable is on LHS") {
    shouldRewrite(
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE m.rating <= movie.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE movie.rating >= m.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite > so that binding variable is on LHS") {
    shouldRewrite(
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE m.rating > movie.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE movie.rating < m.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite >= so that binding variable is on LHS") {
    shouldRewrite(
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE m.rating >= movie.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE movie.rating <= m.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite = so that binding variable is on LHS") {
    shouldRewrite(
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE m.rating = movie.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE movie.rating = m.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should not rewrite if binding variable is on LHS") {
    shouldNotRewrite(
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE movie.rating = m.rating
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite so that binding variable is on LHS for multiple predicates") {
    shouldRewrite(
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE m.rating = movie.rating AND 90 > movie.runtime AND TRUE = movie.isAwesome
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE movie.rating = m.rating AND movie.runtime < 90 AND movie.isAwesome = TRUE
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite boolean property to equals") {
    shouldRewrite(
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable = TRUE
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite multiple boolean properties to equals") {
    shouldRewrite(
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable AND movie.isAwesome AND movie.isReallyGood
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable = TRUE AND movie.isAwesome = TRUE AND movie.isReallyGood = TRUE
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should only rewrite necessary properties") {
    shouldRewrite(
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable = TRUE AND movie.isAvailable
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable = TRUE AND movie.isAvailable = TRUE
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite inverted boolean property to equals") {
    shouldRewrite(
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE NOT movie.isAvailable
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable = FALSE
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite doubly inverted boolean property to equals") {
    shouldRewrite(
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE NOT (NOT movie.isAvailable)
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable = TRUE
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should not rewrite inverted boolean property to equals") {
    shouldNotRewrite(
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE (NOT movie.isAvailable) = TRUE
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should rewrite multiple boolean properties to equals pt 2") {
    shouldRewrite(
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable AND movie.isAvailable = 2 AND NOT movie.isReallyGood AND (NOT (NOT ( NOT movie.isAvailable)))
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin,
      """
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1,2,3]
        |    WHERE movie.isAvailable = TRUE AND movie.isAvailable = 2 AND movie.isReallyGood = FALSE AND movie.isAvailable = FALSE
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should not rewrite inequality if both arguments are on the bound property") {
    shouldNotRewrite(
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE movie.a < movie.b
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  test("should not rewrite equality if both arguments are on the bound property") {
    shouldNotRewrite(
      """
        |MATCH (m: Movie {title:'Snow White'})
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR m.embedding
        |    WHERE movie.a = movie.b
        |    LIMIT 2
        |  )
        |RETURN movie.title AS title, movie.rating AS rating""".stripMargin
    )
  }

  private def shouldRewrite(from: String, to: String): Unit = {
    val exceptionFactory = Neo4jCypherExceptionFactory(from, None)
    val original = parse(Cypher25, from, exceptionFactory).asInstanceOf[Query]
    val expected = parse(Cypher25, to, exceptionFactory).asInstanceOf[Query]
    val result = SearchPredicateNormalizer.instance(original)

    result should equal(expected)
  }

  private def shouldNotRewrite(query: String): Unit = shouldRewrite(query, query)

}
