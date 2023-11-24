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
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RemoveUseRewriterTest extends CypherFunSuite with RewriteTest {

  override val rewriterUnderTest: Rewriter = RemoveUseRewriter.instance

  test("USE x RETURN 1") {
    assertRewrite("USE x RETURN 1", "RETURN 1")
  }

  test("USE GRAPH(x) RETURN 1 as y") {
    assertRewrite(
      """USE GRAPH(x)
        |RETURN 1 as y
      """.stripMargin,
      "RETURN 1 as y"
    )
  }

  test("WITH i USE GRAPH(foo) RETURN i as a") {
    assertRewrite(
      """WITH i USE GRAPH(foo)
        |RETURN i AS a
      """.stripMargin,
      "WITH i RETURN i AS a"
    )
  }

  test("USE foo UNWIND") {
    assertRewrite(
      """USE foo
        |UNWIND [1, 2, 3] AS i
        |CALL {
        |  WITH i
        |  USE foo
        |  RETURN i AS a
        |}
        |RETURN a
      """.stripMargin,
      """UNWIND [1, 2, 3] AS i
        |CALL {
        |  WITH i
        |  RETURN i AS a
        |}
        |RETURN a""".stripMargin
    )
  }

  test("USE foo UNION") {
    assertRewrite(
      """USE foo
        |RETURN 1
        |UNION
        |USE foo
        |RETURN 1""".stripMargin,
      """RETURN 1
        |UNION
        |RETURN 1""".stripMargin
    )
  }

  override protected def getRewrite(originalQuery: String, expectedQuery: String): (Statement, AnyRef) = {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    SemanticChecker.check(
      original,
      SemanticState.clean.withFeatures(SemanticFeature.MultipleGraphs, SemanticFeature.UseAsSingleGraphSelector)
    )
    val result = rewrite(original)
    (expected, result)
  }
}
