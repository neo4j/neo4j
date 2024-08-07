/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.helpers.TestContext
import org.neo4j.cypher.internal.parser.ParserFixture.parser
import org.neo4j.cypher.internal.rewriting.Deprecations.semanticallyDeprecatedFeaturesIn4_X
import org.neo4j.cypher.internal.rewriting.Deprecations.syntacticallyDeprecatedFeaturesIn4_X
import org.neo4j.cypher.internal.rewriting.conditions.noReferenceEqualityAmongVariables
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.DeprecatedHexLiteralSyntax
import org.neo4j.cypher.internal.util.DeprecatedOctalLiteralSyntax
import org.neo4j.cypher.internal.util.DeprecatedPatternExpressionOutsideExistsSyntax
import org.neo4j.cypher.internal.util.DeprecatedPropertyExistenceSyntax
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SyntaxDeprecationWarningsAndReplacementsTest extends CypherFunSuite {

  test("should warn about deprecated octal syntax") {
    check("RETURN 01277") should equal(Set(
      DeprecatedOctalLiteralSyntax(InputPosition(7, 1, 8))
    ))
  }

  test("should warn about deprecated octal syntax (negative literal)") {
    check("RETURN -01277") should equal(Set(
      DeprecatedOctalLiteralSyntax(InputPosition(7, 1, 8))
    ))
  }

  test("should not warn about correct octal syntax") {
    check("RETURN 0o1277") should equal(Set.empty)
  }

  test("should not warn about correct octal syntax  (negative literal)") {
    check("RETURN -0o1277") should equal(Set.empty)
  }

  test("should warn about deprecated hexadecimal syntax") {
    check("RETURN 0X1277") should equal(Set(
      DeprecatedHexLiteralSyntax(InputPosition(7, 1, 8))
    ))
  }

  test("should warn about deprecated hexadecimal syntax (negative literal)") {
    check("RETURN -0X1277") should equal(Set(
      DeprecatedHexLiteralSyntax(InputPosition(7, 1, 8))
    ))
  }

  test("should not warn about correct hexadecimal syntax") {
    check("RETURN 0x1277") should equal(Set.empty)
  }

  test("should not warn about correct hexadecimal syntax  (negative literal)") {
    check("RETURN -0x1277") should equal(Set.empty)
  }

  test("should warn about pattern expression in RETURN clause") {
    check("RETURN ()--()") should equal(Set(
      DeprecatedPatternExpressionOutsideExistsSyntax(InputPosition(7, 1, 8))
    ))
  }

  test("should not warn about pattern expression in exists function") {
    check("WITH 1 AS foo WHERE exists(()--()) RETURN *") should equal(Set.empty)
  }

  test("should not warn about coercion with a pattern expression in WHERE clause") {
    check("WITH 1 AS foo WHERE ()--() RETURN *") should equal(Set.empty)
  }

  test("should not warn about coercion with a pattern expression in boolean expression") {
    check("RETURN NOT ()--()") should equal(Set.empty)
    check("RETURN ()--() AND ()--()--()") should equal(Set.empty)
    check("RETURN ()--() OR ()--()--()") should equal(Set.empty)
  }

  test("should warn about exists() in both union branches") {
    val q = """MATCH (n:Label) WHERE exists(n.prop) RETURN n
              |UNION
              |MATCH (n:OtherLabel) WHERE exists(n.prop) RETURN n
              |""".stripMargin

    check(q) shouldBe Set(
      DeprecatedPropertyExistenceSyntax(InputPosition(22, 1, 23)),
      DeprecatedPropertyExistenceSyntax(InputPosition(79, 3, 28))
    )
  }

  private val plannerName = new PlannerName {
    override def name: String = "fake"
    override def toTextOutput: String = "fake"
    override def version: String = "fake"
  }

  private def check(query: String) = {
    val logger = new RecordingNotificationLogger()
    val statement = parse(query)
    val initialState = InitialState(query, plannerName, new AnonymousVariableNameGenerator, maybeStatement = Some(statement))

    val pipeline =
      SyntaxDeprecationWarningsAndReplacements(syntacticallyDeprecatedFeaturesIn4_X) andThen
        PreparatoryRewriting andThen
        SemanticAnalysis(warn = true) andThen
        SyntaxDeprecationWarningsAndReplacements(semanticallyDeprecatedFeaturesIn4_X)

    val transformedState = pipeline.transform(initialState, TestContext(logger))

    // Check that we didn't introduce any duplicate AST nodes
    noReferenceEqualityAmongVariables(transformedState.statement()) shouldBe empty

    logger.notifications
  }

  private def parse(queryText: String): Statement = parser.parse(queryText.replace("\r\n", "\n"), OpenCypherExceptionFactory(None))

}
