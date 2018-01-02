/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.ast.Statement
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, Rewriter}

trait RewriteTest {
  self: CypherFunSuite =>

  import parser.ParserFixture._

  def rewriterUnderTest: Rewriter
  val semanticChecker = new SemanticChecker

  protected def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    val mkException = new SyntaxExceptionCreator(originalQuery, Some(DummyPosition(0)))
    semanticChecker.check(originalQuery, original, mkException)

    val result = rewrite(original)
    assert(result === expected, "\n" + originalQuery)
  }

  protected def parseForRewriting(queryText: String) = parser.parse(queryText.replace("\r\n", "\n"))

  protected def rewrite(original: Statement): AnyRef =
    original.rewrite(rewriterUnderTest)

  protected def endoRewrite(original: Statement): Statement =
    original.endoRewrite(rewriterUnderTest)

  protected def assertIsNotRewritten(query: String) {
    val original = parser.parse(query)
    val result = original.rewrite(rewriterUnderTest)
    assert(result === original, "\n" + query)
  }
}
