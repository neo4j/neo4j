/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_2.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.v3_2.parser.ParserFixture.parser
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, Rewriter, SemanticCheckResult, SemanticState, inSequence}

class DesugarDesugaredMapProjectionTest extends CypherFunSuite {

  assertRewrite(
    "match (n) return n{k:42} as x",
    "match (n) return n{k:42} as x")

  assertRewrite(
    "match (n) return n{.id} as x",
    "match (n) return n{id: n.id} as x")

  assertRewrite(
    "with '42' as existing match (n) return n{existing} as x",
    "with '42' as existing match (n) return n{existing: existing} as x")

  assertRewrite(
    "match (n) return n{.foo,.bar,.baz} as x",
    "match (n) return n{foo: n.foo, bar: n.bar, baz: n.baz} as x")

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
      |} as x""".stripMargin)

  def assertRewrite(originalQuery: String, expectedQuery: String) {
    test(originalQuery + " is rewritten to " + expectedQuery) {
      def rewrite(q: String): Statement = {
        val mkException = new SyntaxExceptionCreator(originalQuery, InputPosition.NONE)
        val sequence: Rewriter = inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException))
        val originalAst = parser.parse(q).endoRewrite(sequence)
        val semanticCheckResult = originalAst.semanticCheck(SemanticState.clean)
        val withScopes = originalAst.endoRewrite(recordScopes(semanticCheckResult.state))

        withScopes.endoRewrite(desugarMapProjection(semanticCheckResult.state))
      }

      val rewrittenOriginal = rewrite(originalQuery)
      val rewrittenExpected = rewrite(expectedQuery)

      assert(rewrittenOriginal === rewrittenExpected)

    }
  }
}