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
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilationState.{State2, State3, State4}
import org.neo4j.cypher.internal.frontend.v3_2.SemanticState
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement

object SemanticAnalysis {
  def handle(statement: Statement, context: Context): SemanticState = {
    val semanticState = SemanticChecker.check(statement, context.exceptionCreator)
    semanticState.notifications.foreach(context.notificationLogger.log)
    semanticState
  }

  case object Early extends SemanticAnalysis[State2, State3] {
    override def transform(from: State2, context: Context): State3 =
      from.add(handle(from.statement, context))
  }

  case object Late extends SemanticAnalysis[State4, State4] {
    override def transform(from: State4, context: Context): State4 =
      from.copy(semantics = handle(from.statement, context))
  }
}

abstract class SemanticAnalysis[A,B] extends Phase[A,B] {
  override def phase = SEMANTIC_CHECK

  override def why = "Does variable binding, typing, type checking and other semantic checks"
}