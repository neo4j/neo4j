/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.frontend.helpers

import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.{BaseContext, CompilationPhaseTracer, Monitors, devNullLogger}
import org.neo4j.cypher.internal.v4_0.util.{CypherExceptionFactory, OpenCypherExceptionFactory}
import org.scalatest.matchers.{MatchResult, Matcher}

class ErrorCollectingContext extends BaseContext {

  var errors: Seq[SemanticErrorDef] = Seq.empty

  override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING
  override def notificationLogger: devNullLogger.type = devNullLogger
  override def cypherExceptionFactory: CypherExceptionFactory = OpenCypherExceptionFactory(None)
  override def monitors: Monitors = ???
  override def errorHandler: Seq[SemanticErrorDef] => Unit = (errs: Seq[SemanticErrorDef]) =>
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
