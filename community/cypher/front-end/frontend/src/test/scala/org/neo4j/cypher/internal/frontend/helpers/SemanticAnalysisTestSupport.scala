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
package org.neo4j.cypher.internal.frontend.helpers

import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.util._
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.util.Optional
import java.util.UUID

class ErrorCollectingContext(val isComposite: Boolean = false, databaseName: String = "mock") extends BaseContext {

  var errors: Seq[SemanticErrorDef] = Seq.empty

  override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING
  override def notificationLogger: devNullLogger.type = devNullLogger
  override def cypherExceptionFactory: CypherExceptionFactory = OpenCypherExceptionFactory(None)
  override def monitors: Monitors = ???

  override def errorHandler: Seq[SemanticErrorDef] => Unit = (errs: Seq[SemanticErrorDef]) => {
    // As semantic analysis gets run twice in testing, concatenate new errors so all are returned.
    val newErrs = errs.filterNot(errors.contains)
    errors ++= newErrs
  }

  override def errorMessageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

  override def cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled

  override def internalSyntaxUsageStats: InternalSyntaxUsageStats = InternalSyntaxUsageStatsNoOp

  override def sessionDatabase: DatabaseReference = {
    val outerComposite = isComposite
    new DatabaseReference {
      override def alias(): NormalizedDatabaseName = ???

      override def namespace(): Optional[NormalizedDatabaseName] = Optional.empty()

      override def isPrimary: Boolean = true

      override def id(): UUID = ???

      override def toPrettyString: String = s"${fullName().name()} (isComposite: $isComposite)"

      override def fullName(): NormalizedDatabaseName = new NormalizedDatabaseName(databaseName)

      override def compareTo(o: DatabaseReference): Int = 0

      override def isComposite: Boolean = outerComposite

      override def owningDatabaseName: String = ???
    }
  }
}

object ErrorCollectingContext {

  def failWith(errorMessages: String*): Matcher[ErrorCollectingContext] = new Matcher[ErrorCollectingContext] {

    override def apply(context: ErrorCollectingContext): MatchResult = {
      MatchResult(
        matches = context.errors.map(_.msg) == errorMessages,
        rawFailureMessage = s"Expected errors: $errorMessages but got ${context.errors}",
        rawNegatedFailureMessage = s"Did not expect errors: $errorMessages."
      )
    }
  }
}

object NoPlannerName extends PlannerName {
  override def name = "no planner"
  override def toTextOutput = "no planner"
  override def version = "no version"
}
