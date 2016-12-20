/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
import org.neo4j.cypher.internal.compiler.v3_2.SemanticChecker
import org.neo4j.cypher.internal.frontend.v3_2.SemanticState

case class SemanticAnalysis(warn: Boolean) extends Phase {

  override def transform(from: CompilationState, context: Context): CompilationState = {
    val semanticState = SemanticChecker.check(from.statement, context.exceptionCreator)
    if (warn) semanticState.notifications.foreach(context.notificationLogger.log)
    from.copy(maybeSemantics = Some(semanticState))
  }
  override def phase = SEMANTIC_CHECK

  override def description = "do variable binding, typing, type checking and other semantic checks"

  override def postConditions: Set[Condition] = Set(Contains[SemanticState])
}