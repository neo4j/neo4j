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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.helpers.TestContext
import org.neo4j.cypher.internal.rewriting.Deprecations.semanticallyDeprecatedFeatures
import org.neo4j.cypher.internal.rewriting.Deprecations.syntacticallyDeprecatedFeatures
import org.neo4j.cypher.internal.rewriting.conditions.noReferenceEqualityAmongVariables
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.annotation.nowarn

class SyntaxDeprecationWarningsAndReplacementsTest extends CypherFunSuite {

  // Add specific tests for syntax deprecation warning and replacements here.

//  test("placeholder query") {
//    check("MATCH (n) RETURN *") should equal(Set.empty)
//  }

  private val plannerName = new PlannerName {
    override def name: String = "fake"
    override def toTextOutput: String = "fake"
    override def version: String = "fake"
  }

  // This will either be used or removed in https://trello.com/c/sm52Y2yU/588-investigate-if-syntaxdeprecationwarningsandreplacementstest-can-be-removed
  @nowarn("msg=is never used")
  private def check(query: String) = {
    val logger = new RecordingNotificationLogger()
    val statement = parse(query)
    val initialState =
      InitialState(query, plannerName, new AnonymousVariableNameGenerator, maybeStatement = Some(statement))

    val pipeline =
      SyntaxDeprecationWarningsAndReplacements(syntacticallyDeprecatedFeatures) andThen
        PreparatoryRewriting andThen
        SemanticAnalysis(warn = true) andThen
        SyntaxDeprecationWarningsAndReplacements(semanticallyDeprecatedFeatures)

    val transformedState = pipeline.transform(initialState, TestContext(logger))

    // Check that we didn't introduce any duplicate AST nodes
    noReferenceEqualityAmongVariables(transformedState.statement())(CancellationChecker.NeverCancelled) shouldBe empty

    logger.notifications
  }

  private def parse(queryText: String): Statement = JavaCCParser.parse(
    queryText.replace("\r\n", "\n"),
    OpenCypherExceptionFactory(None)
  )

}
