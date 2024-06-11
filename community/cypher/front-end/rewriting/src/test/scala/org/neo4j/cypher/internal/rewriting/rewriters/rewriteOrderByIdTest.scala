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
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class rewriteOrderByIdTest extends CypherFunSuite with AstRewritingTestSupport {

  private val prettifier = Prettifier(ExpressionStringifier(_.asCanonicalStringVal))

  test("Rewrites ORDER BY ID(node)") {
    assertRewrite("MATCH (n) RETURN n ORDER BY id(n)", "MATCH (n) RETURN n ORDER BY n")
    assertRewrite("MATCH (n) RETURN n ORDER BY id(n) ASC", "MATCH (n) RETURN n ORDER BY n ASC")
    assertRewrite("MATCH (n) RETURN n ORDER BY id(n) DESC", "MATCH (n) RETURN n ORDER BY n DESC")
  }

  test("Rewrites ORDER BY ID(relationship)") {
    assertRewrite("MATCH ()-[r]-() RETURN r ORDER BY id(r)", "MATCH ()-[r]-() RETURN r ORDER BY r")
    assertRewrite("MATCH ()-[r]-() RETURN r ORDER BY id(r) ASC", "MATCH ()-[r]-() RETURN r ORDER BY r ASC")
    assertRewrite("MATCH ()-[r]-() RETURN r ORDER BY id(r) DESC", "MATCH ()-[r]-() RETURN r ORDER BY r DESC")
  }

  test("Does not rewrite ORDER BY ID(x) if x cannot be proven to be an entity") {
    assertIsNotRewritten("MATCH (n:L) UNWIND [n,1] AS x RETURN x ORDER BY id(x)")
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    val original = parse(originalQuery, OpenCypherExceptionFactory(None))
    val expected = parse(expectedQuery, OpenCypherExceptionFactory(None))

    val checkResult = original.semanticCheck.run(SemanticState.clean, SemanticCheckContext.default)
    val rewriter = rewriteOrderById(checkResult.state)

    val result = original.rewrite(rewriter)
    assert(
      result === expected,
      s"\n$originalQuery\nshould be rewritten to:\n$expectedQuery\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  private def assertIsNotRewritten(query: String): Unit = {
    val original = parse(query, OpenCypherExceptionFactory(None))

    val checkResult = original.semanticCheck.run(SemanticState.clean, SemanticCheckContext.default)
    val rewriter = rewriteOrderById(checkResult.state)

    val result = original.rewrite(rewriter)
    assert(
      result === original,
      s"\n$query\nshould not have been rewritten but was to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }
}
