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
package org.neo4j.cypher.internal.rewriting.rewriters.astRewriters

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PropertyExistsToIsNotNullTest extends CypherFunSuite with RewriteTest {

  override val rewriterUnderTest: Rewriter =
    PropertyExistsToIsNotNull.getRewriter(
      SemanticState.clean,
      Map.empty,
      new AnonymousVariableNameGenerator,
      CancellationChecker.neverCancelled(),
      CypherVersion.Cypher25
    )

  override protected def assertRewrite(
    version: CypherVersion,
    originalQuery: String,
    expectedQuery: String
  ): Unit = {
    val original = parseForRewriting(version, originalQuery)
    val expected = parseForRewriting(version, expectedQuery)
    val semanticContext = SemanticCheckContext(version, NotImplementedErrorMessageProvider)
    SemanticChecker.check(original, SemanticState.clean, semanticContext)
    val result = rewrite(original, originalQuery)
    val resultStr = prettifier.asString(result.asInstanceOf[Statement])
    val expectedStr = prettifier.asString(expected)
    assert(
      resultStr === expectedStr,
      s"\n$originalQuery\nshould be rewritten to:\n$expectedStr\nbut was rewritten to:\n$resultStr"
    )
  }

  test("PROPERTY_EXISTS(n, prop) rewrites to CASE WHEN n IS NULL THEN NULL ELSE n.prop IS NOT NULL END") {
    assertRewrite(
      CypherVersion.Cypher25,
      "RETURN PROPERTY_EXISTS(n, prop)",
      "RETURN CASE WHEN n IS NULL THEN null ELSE n.prop IS NOT NULL END"
    )
  }

  test("PROPERTY_EXISTS(null, prop) rewrites to CASE WHEN null IS NULL THEN NULL ELSE null.prop IS NOT NULL END") {
    assertRewrite(
      CypherVersion.Cypher25,
      "RETURN PROPERTY_EXISTS(null, prop)",
      "RETURN CASE WHEN `null` IS NULL THEN null ELSE `null`.prop IS NOT NULL END"
    )
  }
}
