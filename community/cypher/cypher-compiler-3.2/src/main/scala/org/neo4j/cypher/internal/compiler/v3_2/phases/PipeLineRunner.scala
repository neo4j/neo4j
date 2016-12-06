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

import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilationState.State1
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v3_2.{CompilationPhaseTracer, InternalNotificationLogger, PreparedQuerySyntax}
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition}

case class PipeLineRunner(exceptionCreator: (String, InputPosition) => CypherException,
                          sequencer: String => RewriterStepSequencer,
                          tracer: CompilationPhaseTracer,
                          notificationLogger: InternalNotificationLogger) {
  private val pipeLine =
      Parsing andThen
      DeprecationWarnings andThen
      PreparatoryRewriting andThen
      SemanticAnalysis andThen
      AstRewriting(sequencer)

  def runIt(rawQueryText: String, offset: Option[InputPosition]): String => PreparedQuerySyntax = {
    val startState = State1(rawQueryText, offset)
    val context = Context(exceptionCreator, tracer, notificationLogger)
    val finalState = pipeLine.transform(startState, context)

    (plannerName) => PreparedQuerySyntax(finalState.statement, rawQueryText, offset, finalState.extractedParams)(plannerName, finalState.postConditions)
  }
}

