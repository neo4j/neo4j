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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.helpers.SyntaxExceptionCreator
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.kernel.database.DatabaseReference

class BaseContextImpl(
  final override val cypherVersion: CypherVersion,
  final override val cypherExceptionFactory: CypherExceptionFactory,
  final override val tracer: CompilationPhaseTracer,
  final override val notificationLogger: InternalNotificationLogger,
  final override val monitors: Monitors,
  final override val cancellationChecker: CancellationChecker,
  final override val internalUsageStats: InternalUsageStats,
  final override val sessionDatabase: DatabaseReference,
  final override val semanticFeatures: Seq[SemanticFeature],
  final override val isScopeQuery: Boolean,
  final override val shadowedFunctions: Set[String],
  final override val isDebugSession: Boolean
) extends BaseContext {

  final override val errorHandler: Seq[SemanticErrorDef] => Unit =
    SyntaxExceptionCreator.throwOnError(cypherExceptionFactory)

  final override def errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
}

object BaseContextImpl {

  def apply(
    cypherVersion: CypherVersion,
    tracer: CompilationPhaseTracer,
    notificationLogger: InternalNotificationLogger,
    queryText: String,
    offset: Option[InputPosition],
    monitors: Monitors,
    cancellationChecker: CancellationChecker,
    internalSyntaxUsageStats: InternalUsageStats,
    sessionDatabase: DatabaseReference,
    semanticFeatures: Seq[SemanticFeature],
    isScopeQuery: Boolean,
    shadowedFunctions: Set[String],
    isDebugSession: Boolean
  ): BaseContextImpl = {
    val exceptionFactory = Neo4jCypherExceptionFactory(queryText, offset)
    new BaseContextImpl(
      cypherVersion,
      exceptionFactory,
      tracer,
      notificationLogger,
      monitors,
      cancellationChecker,
      internalSyntaxUsageStats,
      sessionDatabase,
      semanticFeatures,
      isScopeQuery,
      shadowedFunctions,
      isDebugSession
    )
  }
}
