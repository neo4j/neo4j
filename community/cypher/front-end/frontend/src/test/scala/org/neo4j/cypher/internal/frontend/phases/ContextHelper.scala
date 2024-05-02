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

import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.devNullLogger
import org.scalatestplus.mockito.MockitoSugar

object ContextHelper extends MockitoSugar {

  def create(outerTargetsComposite: Boolean = false, sessionDatabase: String = null): BaseContext = {
    new BaseContext {
      override def tracer: CompilationPhaseTracer = NO_TRACING

      override def notificationLogger: InternalNotificationLogger = devNullLogger

      override def cypherExceptionFactory: CypherExceptionFactory = OpenCypherExceptionFactory(None)

      override def monitors: Monitors = mock[Monitors]

      override def errorHandler: Seq[SemanticErrorDef] => Unit =
        (errors: Seq[SemanticErrorDef]) =>
          errors.foreach(e => throw cypherExceptionFactory.syntaxException(e.msg, e.position))

      override def errorMessageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

      override def cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled

      override def internalSyntaxUsageStats: InternalSyntaxUsageStats = InternalSyntaxUsageStatsNoOp

      override def targetsComposite: Boolean = outerTargetsComposite

      override def sessionDatabaseName: String = sessionDatabase
    }
  }
}
