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

import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.scalatestplus.mockito.MockitoSugar.mock

//noinspection TypeAnnotation
case class TestContext(
  override val notificationLogger: InternalNotificationLogger = mock[InternalNotificationLogger],
  override val targetsComposite: Boolean = false,
  override val sessionDatabaseName: String = null
) extends BaseContext {
  override def tracer = CompilationPhaseTracer.NO_TRACING

  override def cypherExceptionFactory: CypherExceptionFactory = OpenCypherExceptionFactory(None)

  override def monitors = mock[Monitors]

  override def errorHandler = _ => ()

  override def errorMessageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

  override def cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled

  override def internalSyntaxUsageStats: InternalSyntaxUsageStats = InternalSyntaxUsageStatsNoOp
}
