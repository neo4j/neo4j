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
import org.neo4j.cypher.internal.CypherVersionHelpers
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.Rewriter
import org.scalatest.Assertions
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

trait RewriteTest extends AstRewritingTestSupport {
  // CypherFunSuite (2.13) and CypherFunSuite both satisfy this while front-end is still on 2.13.
  self: AnyFunSuiteLike with Assertions with Matchers =>

  def sendStatementToRewriterConstructor: Boolean = false

  def rewriterUnderTest: Rewriter
  def rewriterUnderTest(query: String): Rewriter = rewriterUnderTest
  def rewriterUnderTest(query: Statement): Rewriter = rewriterUnderTest

  val prettifier = Prettifier(
    ExpressionStringifier((e: Expression) => e.asCanonicalStringVal)
  )

  // Avoid ScalaTest macro assert: this trait is compiled into the Scala 3 test-jar but also
  // mixed into Scala 2.13 front-end tests, which load ScalaTest 2.13 at runtime.
  private def assertSameRewrite(actual: Any, expected: Any, clue: => String): Unit =
    if (!(actual === expected)) throw new AssertionError(clue)

  protected def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    val (expected, result) = getRewrite(originalQuery, expectedQuery)
    assertSameRewrite(
      result,
      expected,
      s"\n$originalQuery\nshould be rewritten to:\n${prettifier.asString(expected)}\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  // additionalExpectedAstUpdates is for updating things that are changed in the rewriter but cannot be expressed in the query,
  // for example the AddedInRewriteProcCall flag on WITH
  protected def assertRewrite(
    originalQuery: String,
    expectedQuery: String,
    additionalExpectedAstUpdates: Statement => Statement
  ): Unit = {
    val (expected, result) = getRewrite(originalQuery, expectedQuery)
    val updatedExpected = additionalExpectedAstUpdates(expected)
    assertSameRewrite(
      result,
      updatedExpected,
      s"\n$originalQuery\nshould be rewritten to:\n${prettifier.asString(updatedExpected)}\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  protected def assertRewrite(version: CypherVersion, originalQuery: String, expectedQuery: String): Unit = {
    val (expected, result) = getRewrite(version, originalQuery, expectedQuery)
    assertSameRewrite(
      result,
      expected,
      s"\n$originalQuery\nshould be rewritten to:\n${prettifier.asString(expected)}\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  protected def assertRewrite(
    version: CypherVersion,
    originalQuery: String,
    expectedQuery: String,
    additionalExpectedAstUpdates: Statement => Statement
  ): Unit = {
    val (expected, result) = getRewrite(version, originalQuery, expectedQuery)
    val updatedExpected = additionalExpectedAstUpdates(expected)
    assertSameRewrite(
      result,
      updatedExpected,
      s"\n$originalQuery\nshould be rewritten to:\n${prettifier.asString(updatedExpected)}\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  protected def assertRewriteWithFeatures(originalQuery: String, expectedQuery: String): Unit = {
    val (expected, result) = getRewriteWithFeatures(originalQuery, expectedQuery)
    assertSameRewrite(
      result,
      expected,
      s"\n$originalQuery\nshould be rewritten to:\n${prettifier.asString(expected)}\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  protected def assertRewriteWithFeatures(
    version: CypherVersion,
    originalQuery: String,
    expectedQuery: String
  ): Unit = {
    val (expected, result) = getRewriteWithFeatures(version, originalQuery, expectedQuery)
    assertSameRewrite(
      result,
      expected,
      s"\n$originalQuery\nshould be rewritten to:\n${prettifier.asString(expected)}\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  protected def assertRewriteWithFeaturesCompareStrings(
    version: CypherVersion,
    originalQuery: String,
    expectedQuery: String
  ): Unit = {
    val (expected, result) = getRewriteWithFeatures(version, originalQuery, expectedQuery)
    assertSameRewrite(
      prettifier.asString(expected),
      prettifier.asString(result.asInstanceOf[Statement]),
      s"\n$originalQuery\nshould be rewritten to:\n${prettifier.asString(expected)}\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  protected def getRewrite(
    version: CypherVersion,
    originalQuery: String,
    expectedQuery: String
  ): (Statement, AnyRef) = {
    val original = parseForRewriting(version, originalQuery)
    val expected = parseForRewriting(version, expectedQuery)
    val semanticContext = SemanticCheckContext(version, NotImplementedErrorMessageProvider)
    SemanticChecker.check(original, SemanticState.clean, semanticContext)
    val result = rewrite(original, originalQuery)
    (expected, result)
  }

  protected def getRewrite(originalQuery: String, expectedQuery: String): (Statement, AnyRef) = {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    val semanticContext =
      SemanticCheckContext(CypherVersionHelpers.randomVersion(), NotImplementedErrorMessageProvider)
    SemanticChecker.check(original, SemanticState.clean, semanticContext)
    val result = rewrite(original, originalQuery)
    (expected, result)
  }

  protected def getRewriteWithFeatures(originalQuery: String, expectedQuery: String): (Statement, AnyRef) = {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    SemanticChecker.check(
      original,
      SemanticState.clean.withFeatures(getFeatures()),
      SemanticCheckContext(CypherVersionHelpers.randomVersion(), NotImplementedErrorMessageProvider)
    )
    val result = rewrite(original, originalQuery)
    (expected, result)
  }

  protected def getRewriteWithFeatures(
    version: CypherVersion,
    originalQuery: String,
    expectedQuery: String
  ): (Statement, AnyRef) = {
    val original = parseForRewriting(version, originalQuery)
    val expected = parseForRewriting(version, expectedQuery)
    SemanticChecker.check(
      original,
      SemanticState.clean.withFeatures(getFeatures()),
      SemanticCheckContext(version, NotImplementedErrorMessageProvider)
    )
    val result = rewrite(original, originalQuery)
    (expected, result)
  }

  protected def getFeatures(): Seq[SemanticFeature] =
    Seq(SemanticFeature.MultipleGraphs, SemanticFeature.UseAsSingleGraphSelector)

  protected def parseForRewriting(version: CypherVersion, queryText: String): Statement = {
    val preparedQuery = queryText.replace("\r\n", "\n")
    parse(version, preparedQuery, Neo4jCypherExceptionFactory(queryText, None))
  }

  protected def parseForRewriting(queryText: String): Statement = {
    val preparedQuery = queryText.replace("\r\n", "\n")
    parse(preparedQuery, Neo4jCypherExceptionFactory(queryText, None))
  }

  protected def rewrite(original: Statement): AnyRef =
    original.rewrite(rewriterUnderTest(""))

  protected def rewrite(original: Statement, queryString: String): AnyRef =
    original.rewrite(
      if (sendStatementToRewriterConstructor) rewriterUnderTest(original)
      else rewriterUnderTest(queryString)
    )

  protected def endoRewrite(original: Statement): Statement =
    original.endoRewrite(rewriterUnderTest(""))

  protected def endoRewrite(original: Statement, queryString: String): Statement =
    original.endoRewrite(rewriterUnderTest(queryString))

  protected def assertIsNotRewritten(query: String): Unit = {
    val original = parse(query, Neo4jCypherExceptionFactory(query, None))
    val result = original.rewrite(
      if (sendStatementToRewriterConstructor) rewriterUnderTest(original)
      else rewriterUnderTest
    )
    assertSameRewrite(
      result,
      original,
      s"\n$query\nshould not have been rewritten but was to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  protected def assertIsNotRewritten(version: CypherVersion, query: String): Unit = {
    val original = parse(version, query, Neo4jCypherExceptionFactory(query, None))
    val result = original.rewrite(
      if (sendStatementToRewriterConstructor) rewriterUnderTest(original)
      else rewriterUnderTest
    )
    assertSameRewrite(
      result,
      original,
      s"\n$query\nshould not have been rewritten but was to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }
}
