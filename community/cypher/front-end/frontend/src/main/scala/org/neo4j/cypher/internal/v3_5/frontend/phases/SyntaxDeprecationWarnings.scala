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
package org.neo4j.cypher.internal.v3_5.frontend.phases

import org.neo4j.cypher.internal.v3_5.ast.Statement
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer.CompilationPhase.DEPRECATION_WARNINGS
import org.neo4j.cypher.internal.v3_5.rewriting.Deprecations
import org.neo4j.cypher.internal.v3_5.util.InternalNotification

case class SyntaxDeprecationWarnings(deprecations: Deprecations) extends VisitorPhase[BaseContext, BaseState] {
  override def visit(state: BaseState, context: BaseContext): Unit = {
    val warnings = findDeprecations(state.statement())

    warnings.foreach(context.notificationLogger.log)
  }

  private def findDeprecations(statement: Statement): Set[InternalNotification] =
    statement.fold(Set.empty[InternalNotification])(
      deprecations.find.andThen(deprecation => acc => acc ++ deprecation.generateNotification())
    )

  override def phase = DEPRECATION_WARNINGS

  override def description = "find deprecated Cypher constructs and generate warnings for them"
}
