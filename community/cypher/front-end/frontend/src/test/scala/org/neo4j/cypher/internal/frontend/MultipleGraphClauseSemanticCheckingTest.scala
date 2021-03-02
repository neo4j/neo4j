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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.frontend.helpers.TestContext
import org.neo4j.cypher.internal.frontend.helpers.TestState
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.parser
import org.neo4j.cypher.internal.parser.ParserTest
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.parboiled.scala.Rule1

class MultipleGraphClauseSemanticCheckingTest
  extends ParserTest[ast.Statement, SemanticCheckResult] with parser.Statement {

  // INFO: Use result.dumpAndExit to debug these tests

  implicit val parser: Rule1[Statement] = Statement

  test("FROM requires feature") {
    parsingWith("""FROM g RETURN 1""", multiGraph)
      .shouldVerify(_.errorMessages.shouldEqual(Set("The `FROM GRAPH` clause is not available in this implementation of Cypher due to lack of support for FROM graph selector.")))
    parsingWith("""FROM g RETURN 1""", defaultFeatures)
      .shouldVerify(_.errorMessages.shouldEqual(Set()))
  }

  test("USE requires feature") {
    parsingWith("""USE g RETURN 1""", multiGraph)
      .shouldVerify(_.errorMessages.shouldEqual(Set("The `USE GRAPH` clause is not available in this implementation of Cypher due to lack of support for USE graph selector.")))
    parsingWith("""USE g RETURN 1""", useGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set()))
  }

  test("Allow single identifier in FROM/USE") {
    parsingWith("FROM x RETURN 1", fromGraphSelector)
      .shouldVerify(_.errors.shouldEqual(Seq()))
    parsingWith("USE x RETURN 1", useGraphSelector)
      .shouldVerify(_.errors.shouldEqual(Seq()))
  }

  test("Allow qualified identifier in FROM/USE") {
    parsingWith("FROM x.y.z RETURN 1", fromGraphSelector)
      .shouldVerify(_.errors.shouldEqual(Seq()))
    parsingWith("USE x.y.z RETURN 1", useGraphSelector)
      .shouldVerify(_.errors.shouldEqual(Seq()))
  }

  test("Allow view invocation in FROM/USE") {
    parsingWith("FROM v(g, w(k)) RETURN 1", fromGraphSelector)
      .shouldVerify(_.errors.shouldEqual(Seq()))
    parsingWith("USE v(g, w(k)) RETURN 1", useGraphSelector)
      .shouldVerify(_.errors.shouldEqual(Seq()))
  }

  test("Allow qualified view invocation in FROM/USE") {
    parsingWith("FROM a.b.v(g, x.g, x.v(k)) RETURN 1", fromGraphSelector)
      .shouldVerify(_.errors.shouldEqual(Seq()))
    parsingWith("USE a.b.v(g, x.g, x.v(k)) RETURN 1", useGraphSelector)
      .shouldVerify(_.errors.shouldEqual(Seq()))
  }

  test("Do not allow arbitrary expressions in FROM/USE") {
    parsingWith("FROM 1 RETURN 1", fromGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))
    parsingWith("USE 1 RETURN 1", useGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))

    parsingWith("FROM 'a' RETURN 1", fromGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))
    parsingWith("USE 'a' RETURN 1", useGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))

    parsingWith("FROM [x] RETURN 1", fromGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))
    parsingWith("USE [x] RETURN 1", useGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))

    parsingWith("FROM 1 + 2 RETURN 1", fromGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))
    parsingWith("USE 1 + 2 RETURN 1", useGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))
  }

  test("Disallow expressions in view invocations") {
    parsingWith("FROM a.b.v(1, 1+2, 'x') RETURN 1", fromGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))
    parsingWith("USE a.b.v(1, 1+2, 'x') RETURN 1", useGraphSelector)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Invalid graph reference")))
  }

  test("Allow expressions in view invocations (with feature flag)") {
    parsingWith("WITH 1 AS x FROM v(2, 'x', x, x+3) RETURN 1", fromGraphSelector ++ expressionsInViews)
      .shouldVerify(_.errors.shouldEqual(Seq()))
    parsingWith("WITH 1 AS x USE v(2, 'x', x, x+3) RETURN 1", useGraphSelector ++ expressionsInViews)
      .shouldVerify(_.errors.shouldEqual(Seq()))
  }

  test("Expressions in view invocations are checked (with feature flag)") {
    parsingWith("WITH 1 AS x FROM v(2, 'x', y, x+3) RETURN 1", fromGraphSelector ++ expressionsInViews)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Variable `y` not defined")))
    parsingWith("WITH 1 AS x USE v(2, 'x', y, x+3) RETURN 1", useGraphSelector ++ expressionsInViews)
      .shouldVerify(_.errorMessages.shouldEqual(Set("Variable `y` not defined")))
  }

  override type Extra = Seq[SemanticFeature]

  private val multiGraph = Seq(
    SemanticFeature.MultipleGraphs,
    SemanticFeature.WithInitialQuerySignature,
  )

  private val fromGraphSelector =
    multiGraph :+ SemanticFeature.FromGraphSelector

  private val useGraphSelector =
    multiGraph :+ SemanticFeature.UseGraphSelector

  private val defaultFeatures =
    fromGraphSelector

  private val expressionsInViews = Seq(
    SemanticFeature.ExpressionsInViewInvocations
  )



  override def convert(astNode: ast.Statement): SemanticCheckResult =
    convert(astNode, defaultFeatures)

  override def convert(astNode: Statement, features: Seq[SemanticFeature]): SemanticCheckResult = {
    val rewritten = PreparatoryRewriting(Deprecations.V1).transform(TestState(Some(astNode)), TestContext()).statement()
    val initialState =
      SemanticState.clean.withFeatures(features: _*)
    rewritten.semanticCheck(initialState)
  }

  implicit final class RichSemanticCheckResult(val result: SemanticCheckResult) {
    def state: SemanticState = result.state

    def errors: Seq[SemanticErrorDef] = result.errors

    def errorMessages: Set[String] = errors.map(_.msg).toSet
  }
}
