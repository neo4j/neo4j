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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionHelpers.randomVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.frontend.SemanticAnalysisTestSuite.initialStateWithQuery
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.notification.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N57
import org.scalatest.exceptions.TestFailedException

import scala.util.Success
import scala.util.Try

class SemanticAnalysisTestSuiteTest extends CypherFunSuite with SemanticAnalysisTestSuite {

  test("success assertion") {
    val randVersion = randomVersion()
    val assertion = run("return 1")
    val gqlError = getGql42001_42N57("Bla", 17, 2, 8)

    intercept[TestFailedException](assertion.hasError(gqlError, "hej", p(0)))
    intercept[TestFailedException](assertion.hasErrors(
      gqlError,
      "hej",
      p(0),
      gqlError,
      "hej",
      p(0)
    ))
    intercept[TestFailedException](assertion.hasGQLErrorsIn {
      case `randVersion` => Seq((gqlError, "hej", p(0)))
      case _             => Seq.empty
    })
    intercept[TestFailedException](assertion.hasSemanticErrorsIn {
      case `randVersion` => Seq(SemanticError(gqlError, "hej", p(0)))
      case _             => Seq.empty
    })
    intercept[TestFailedException](assertion.hasNotificationsIn {
      case `randVersion` => Seq(DeprecatedFunctionNotification(p(0), "old", Some("new")))
      case _             => Seq.empty
    })
    intercept[TestFailedException](assertion.failsWithMessageContaining("hej"))
    intercept[TestFailedException](assertion.hasNotifications(DeprecatedFunctionNotification(p(0), "old", Some("new"))))
    intercept[TestFailedException](assertion.failsWithMessageContaining("hej"))
  }

  test("partial success assertion") {
    val randVersion = randomVersion()
    val gqlError = getGql42001_42N57("Bla", 17, 2, 8)

    val assertion = createAssertions {
      case `randVersion` => run("invalid query!!!").results(randVersion)
      case v             => run("return 1").results(v)
    }
    assertion.assertTryIn {
      case `randVersion` => res => res.isFailure shouldBe true
      case _             => res => res.isSuccess shouldBe true
    }
    intercept[TestFailedException](assertion.assertIn {
      case `randVersion` => res => res.errors shouldBe Seq.empty
      case _             => res => res.errors shouldBe Seq.empty
    })
    intercept[TestFailedException](assertion.assertTry(t => t.isSuccess shouldBe true))
    intercept[TestFailedException](assertion.hasNoErrors)
    intercept[TestFailedException](assertion.hasNoNotifications)
    intercept[TestFailedException](assertion.hasError(gqlError, "hej", p(0)))
    intercept[TestFailedException](assertion.hasErrors(
      gqlError,
      "hej",
      p(0),
      gqlError,
      "hej",
      p(0)
    ))
    intercept[TestFailedException](assertion.hasErrors(SemanticError(gqlError, "hej", p(1, 2, 3))))
    intercept[TestFailedException](assertion.hasGQLErrorsIn {
      case `randVersion` => Seq((gqlError, "hej", p(0)))
      case _             => Seq.empty
    })
    intercept[TestFailedException](assertion.hasSemanticErrorsIn {
      case `randVersion` => Seq(SemanticError(gqlError, "hej", p(0)))
      case _             => Seq.empty
    })
    intercept[TestFailedException](assertion.hasNotificationsIn {
      case `randVersion` => Seq(DeprecatedFunctionNotification(p(0), "old", Some("new")))
      case _             => Seq.empty
    })
    intercept[TestFailedException](assertion.failsWithMessageContaining("Invalid input"))
    intercept[TestFailedException](assertion.failsWithMessageContaining("hej"))
    intercept[TestFailedException](assertion.hasNotifications(DeprecatedFunctionNotification(p(0), "old", Some("new"))))
    intercept[TestFailedException](assertion.failsWithMessageContaining("hej"))
  }

  test("partial error") {
    val randVersion = randomVersion()
    val gqlError = getGql42001_42N57("Bla", 17, 2, 8)

    val assertion = createAssertions {
      case `randVersion` => Success(result(randVersion, Seq(SemanticError(gqlError, "hej", p(0))), Seq()))
      case v             => Success(result(v, Seq.empty, Seq.empty))
    }
    assertion.hasGQLErrorsIn {
      case `randVersion` => Seq((gqlError, "hej", p(0)))
      case _             => Seq.empty
    }
    assertion.hasNoNotifications
    intercept[TestFailedException](assertion.hasGQLErrorsIn {
      case `randVersion` => Seq((gqlError, "hej", p(1)))
      case _             => Seq.empty
    })
    intercept[TestFailedException](assertion.hasError(gqlError, "hej", p(1, 2, 3)))
    intercept[TestFailedException](assertion.hasError(gqlError, "hej", p(0)))
  }

  test("one error") {
    val gqlError = getGql42001_42N57("Bla", 17, 2, 8)
    val assertion = createAssertions {
      v => Success(result(v, Seq(SemanticError(gqlError, "hej", p(0))), Seq()))
    }
    assertion.hasNoNotifications
    assertion.hasError(gqlError, "hej", p(0))
    intercept[TestFailedException](assertion.hasError(gqlError, "hej", p(1)))
    intercept[TestFailedException](assertion.hasError(gqlError, "hej?", p(0)))
  }

  test("one notification") {
    val gqlError = getGql42001_42N57("Bla", 17, 2, 8)
    val assertion = createAssertions {
      v => Success(result(v, Seq.empty, Seq(DeprecatedFunctionNotification(p(0), "old", Some("new")))))
    }
    assertion.hasNoErrors
    assertion.hasNotifications(DeprecatedFunctionNotification(p(0), "old", Some("new")))
    intercept[TestFailedException](assertion.hasNotifications(DeprecatedFunctionNotification(p(1), "old", Some("new"))))
    intercept[TestFailedException](assertion.hasNotifications(DeprecatedFunctionNotification(
      p(0),
      "old?",
      Some("new")
    )))
    intercept[TestFailedException](assertion.hasNotifications(DeprecatedFunctionNotification(
      p(0),
      "old",
      Some("new?")
    )))
    intercept[TestFailedException](assertion.hasNoNotifications)
    intercept[TestFailedException](assertion.hasError(gqlError, "hej", p(0)))
  }

  private def createAssertions(f: CypherVersion => Try[SemanticAnalysisResult]): AnalysisAssertions = {
    AnalysisAssertions(
      analyse("dummy query"),
      CypherVersion.values()
        .map(v => v -> f(v))
        .toMap
    )
  }

  private def result(
    v: CypherVersion,
    errors: Seq[SemanticErrorDef],
    notifications: Seq[InternalNotification]
  ): SemanticAnalysisResult = {
    val errContext = new ErrorCollectingContext(v)
    errContext.errorHandler.apply(errors)
    val state = initialStateWithQuery("dummy")
      .withSemanticState(SemanticState.clean.copy(notifications = notifications.toSet))
    SemanticAnalysisResult(errContext, state)
  }
}
