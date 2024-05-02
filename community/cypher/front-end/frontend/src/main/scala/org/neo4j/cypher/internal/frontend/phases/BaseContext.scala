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
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InternalNotificationLogger

trait BaseContext {
  def tracer: CompilationPhaseTracer
  def notificationLogger: InternalNotificationLogger
  def cypherExceptionFactory: CypherExceptionFactory
  def monitors: Monitors
  def errorHandler: Seq[SemanticErrorDef] => Unit

  def errorMessageProvider: ErrorMessageProvider

  def cancellationChecker: CancellationChecker

  def internalSyntaxUsageStats: InternalSyntaxUsageStats

  def targetsComposite: Boolean
  def sessionDatabaseName: String

}
