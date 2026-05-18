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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.RemoveSyntaxTracking
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RemoveSyntaxTrackingTest extends CypherFunSuite with AstRewritingTestSupport {

  private val prettifier = Prettifier(
    ExpressionStringifier((e: Expression) => e.asCanonicalStringVal)
  )

  private val rewriterUnderTest: Rewriter = RemoveSyntaxTracking.instance

  test("remove tracking of escaped variable") {
    assertRewriteForEachVersion(
      "MATCH (`x`) RETURN x",
      "MATCH (x) RETURN x"
    )
    assertRewriteForEachVersion(
      "WITH 123 AS b MATCH (`x`) WHERE x IS NOT NULL AND `b` > 1 RETURN (`x` < x) as `y`",
      "WITH 123 AS b MATCH (x) WHERE x IS NOT NULL AND b > 1 RETURN (x < x) as y"
    )
    assertRewriteForEachVersion(
      "WITH 123 AS b MATCH (`x`) WHERE (x) IS NOT NULL AND `b` > 1 RETURN ((`x`) < (x)) as `y`",
      "WITH 123 AS b MATCH (x) WHERE x IS NOT NULL AND b > 1 RETURN (x < x) as y"
    )
  }

  test("remove tracking of type predicate with double colon only") {
    assertRewriteForEachVersion(
      "RETURN b :: INTEGER AS x",
      "RETURN b IS :: INTEGER AS x"
    )
    assertRewriteForEachVersion(
      """WITH 123 AS b WHERE b :: INTEGER
        |RETURN
        |  CASE WHEN b IS :: STRING THEN 1
        |       WHEN b :: INTEGER THEN 2
        |       WHEN b IS TYPED FLOAT THEN 3
        |       ELSE 4 END AS x""".stripMargin,
      """WITH 123 AS b WHERE b IS :: INTEGER
        |RETURN
        |  CASE WHEN b IS :: STRING THEN 1
        |       WHEN b IS :: INTEGER THEN 2
        |       WHEN b IS :: FLOAT THEN 3
        |       ELSE 4 END AS x""".stripMargin
    )
  }

  test("remove tracking of parenthesized label expression predicate") {
    assertRewriteForEachVersion(
      "RETURN (n:L) AS x",
      "RETURN n:L AS x"
    )
    assertRewriteForEachVersion(
      """MATCH (n) WHERE (n:A:B) AND n:B
        |WITH n, [n,n] AS list WHERE n:C OR (n:D|E)
        |RETURN (n:E&F) AS x, (list[0]:F) AS y""".stripMargin,
      """MATCH (n) WHERE n:A:B AND n:B
        |WITH n, [n,n] AS list WHERE n:C OR n:D|E
        |RETURN n:E&F AS x, list[0]:F AS y""".stripMargin
    )
  }

  def assertRewriteForEachVersion(originalQuery: String, expectedQuery: String): Unit = {
    val exceptionFactory: CypherExceptionFactory = Neo4jCypherExceptionFactory(originalQuery, None)

    CypherVersion.values().foreach { version =>
      val original = parse(version, originalQuery, exceptionFactory)
      val expected = parse(version, expectedQuery, exceptionFactory)
      val semanticContext = SemanticCheckContext(version, NotImplementedErrorMessageProvider)
      SemanticChecker.check(original, SemanticState.clean, semanticContext)
      val result = original.rewrite(rewriterUnderTest)
      assert(
        result === expected,
        s"""In Cypher$version
           |$originalQuery
           |should be rewritten to:
           |${prettifier.asString(expected)}
           |but was rewritten to:
           |${prettifier.asString(result.asInstanceOf[Statement])}""".stripMargin
      )
    }
  }
}
