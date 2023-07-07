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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class ReplacePatternComprehensionWithCollectSubqueryTest extends CypherFunSuite with RewriteTest with TestName {

  override def rewriterUnderTest: Rewriter = inSequence(
    ReplacePatternComprehensionWithCollectSubquery(new AnonymousVariableNameGenerator()).instance,
    removeGeneratedNamesAndParamsOnTree
  )

  private def fullQuery(expression: String) =
    s"RETURN $expression AS foo"

  override protected def assertRewrite(originalQuery: String, expectedQuery: String): Unit =
    super.assertRewrite(fullQuery(originalQuery), fullQuery(expectedQuery))

  override protected def assertIsNotRewritten(query: String): Unit =
    super.assertIsNotRewritten(fullQuery(query))

  override protected def getRewrite(originalQuery: String, expectedQuery: String): (Statement, AnyRef) = {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    val result = rewrite(original)
    (expected, result)
  }

  test("[(n)--(m) | n.prop]") {
    assertRewrite(
      testName,
      """COLLECT { 
        |  MATCH (n)--(m)
        |  RETURN n.prop AS anon_0 
        |}""".stripMargin
    )
  }

  test("[(n:N)-[:R]->()<-[r:R|Q]-(m:M) | n.prop + m.prop]") {
    assertRewrite(
      testName,
      """COLLECT { 
        |  MATCH (n:N)-[:R]->()<-[r:R|Q]-(m:M) 
        |  RETURN n.prop + m.prop AS anon_0 
        |}""".stripMargin
    )
  }

  test("[p = (n)--(m) | p]") {
    assertRewrite(
      testName,
      """COLLECT { 
        |  MATCH anon_0 = (n)--(m)
        |  RETURN anon_0 AS anon_1 
        |}""".stripMargin
    )
  }

  test("[p = (n)--(m) WHERE nodes(p)[0].prop IS NOT NULL | p]") {
    assertRewrite(
      testName,
      """COLLECT { 
        |  MATCH anon_0 = (n)--(m) WHERE nodes(anon_0)[0].prop IS NOT NULL
        |  RETURN anon_0 AS anon_1 
        |}""".stripMargin
    )
  }

  test("[p = (n)--(m {prop: nodes(p)[0].prop}) | p]") {
    assertRewrite(
      testName,
      """COLLECT { 
        |  MATCH anon_0 = (n)--(m {prop: nodes(anon_0)[0].prop})
        |  RETURN anon_0 AS anon_1 
        |}""".stripMargin
    )
  }

  test("[p = (n)--(m) | nodes(p)[0].prop]") {
    assertRewrite(
      testName,
      """COLLECT { 
        |  MATCH anon_0 = (n)--(m)
        |  RETURN nodes(anon_0)[0].prop AS anon_1 
        |}""".stripMargin
    )
  }

  test("[(n)--(m) WHERE m.prop = 0 | n.prop]") {
    assertRewrite(
      testName,
      """COLLECT {
        |  MATCH (n)--(m) WHERE m.prop = 0
        |  RETURN n.prop AS anon_0
        |}""".stripMargin
    )
  }
}
