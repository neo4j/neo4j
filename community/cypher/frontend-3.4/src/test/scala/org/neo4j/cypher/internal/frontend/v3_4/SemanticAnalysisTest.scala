/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4

import org.neo4j.cypher.internal.frontend.v3_4.ErrorCollectingContext.failWith
import org.neo4j.cypher.internal.frontend.v3_4.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

class SemanticAnalysisTest extends CypherFunSuite with AstConstructionTestSupport {

  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  val pipeline = Parsing andThen SemanticAnalysis(warn = true) andThen SemanticAnalysis(warn = false)

  test("can inject starting semantic state") {
    val query = "RETURN name AS name"
    val startState = initStartState(query, Map("name" -> CTString))

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("can inject starting semantic state for larger query") {
    val query = "MATCH (n:Label {name: name}) WHERE n.age > age RETURN n.name AS name"

    val startState = initStartState(query, Map("name" -> CTString, "age" -> CTInteger))
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("should fail for max() with no arguments") {
    val query = "RETURN max() AS max"

    val startState = initStartState(query, Map.empty)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("Insufficient parameters for function 'max'")
  }

  private def initStartState(query: String, initialFields: Map[String, CypherType]) =
    InitialState(query, None, NoPlannerName, initialFields)
}

class ErrorCollectingContext extends BaseContext {

  var errors: Seq[SemanticErrorDef] = Seq.empty

  override def tracer = CompilationPhaseTracer.NO_TRACING
  override def notificationLogger = devNullLogger
  override def exceptionCreator = ???
  override def monitors = ???
  override def errorHandler = (errs: Seq[SemanticErrorDef]) =>
    errors = errs
}

object ErrorCollectingContext {
  def failWith(errorMessages: String*): Matcher[ErrorCollectingContext] = new Matcher[ErrorCollectingContext] {
    override def apply(context: ErrorCollectingContext): MatchResult = {
      MatchResult(
        matches = context.errors.map(_.msg) == errorMessages,
        rawFailureMessage = s"Expected errors: $errorMessages but got ${context.errors}",
        rawNegatedFailureMessage = s"Did not expect errors: $errorMessages.")
    }
  }
}
object NoPlannerName extends PlannerName {
  override def name = "no planner"
  override def toTextOutput = "no planner"
  override def version = "no version"
}
