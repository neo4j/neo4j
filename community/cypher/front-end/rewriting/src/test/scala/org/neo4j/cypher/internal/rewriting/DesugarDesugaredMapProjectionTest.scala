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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.desugarMapProjection
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class DesugarDesugaredMapProjectionTest extends CypherFunSuite with AstRewritingTestSupport {

  assertRewrite(
    "match (n) return n{k:42} as x",
    "match (n) return n{k:42} as x"
  )

  assertRewrite(
    "match (n) return n{.id} as x",
    "match (n) return n{id: n.id} as x"
  )

  assertRewrite(
    "with '42' as existing match (n) return n{existing} as x",
    "with '42' as existing match (n) return n{existing: existing} as x"
  )

  assertRewrite(
    "match (n) return n{.foo,.bar,.baz} as x",
    "match (n) return n{foo: n.foo, bar: n.bar, baz: n.baz} as x"
  )

  assertRewrite(
    "match (n) return n{.*, .apa} as x",
    "match (n) return n{.*, apa: n.apa} as x"
  )

  assertRewrite(
    """match (n), (m)
      |return n {
      | .foo,
      | .bar,
      | inner: m {
      |   .baz,
      |   .apa
      | }
      |} as x""".stripMargin,
    """match (n), (m)
      |return n {
      | foo: n.foo,
      | bar: n.bar,
      | inner: m {
      |   baz: m.baz,
      |   apa: m.apa
      | }
      |} as x""".stripMargin
  )

  def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    test(originalQuery + " is rewritten to " + expectedQuery) {
      def rewrite(q: String): Statement = {
        val exceptionFactory = OpenCypherExceptionFactory(None)
        val sequence: Rewriter = inSequence(normalizeWithAndReturnClauses(exceptionFactory))
        val originalAst = parse(q, exceptionFactory).endoRewrite(sequence)
        val semanticCheckResult = originalAst.semanticCheck.run(SemanticState.clean, SemanticCheckContext.default)
        val withScopes = originalAst.endoRewrite(computeDependenciesForExpressions(semanticCheckResult.state))

        withScopes.endoRewrite(desugarMapProjection.instance)
      }

      val rewrittenOriginal = rewrite(originalQuery)
      val rewrittenExpected = rewrite(expectedQuery)

      assert(rewrittenOriginal === rewrittenExpected)
    }
  }
}
