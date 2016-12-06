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

import org.neo4j.cypher.internal.compiler.v3_2.ASTRewriter
import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilationState.{State3, State4}
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterStepSequencer

case class AstRewriting(sequencer: String => RewriterStepSequencer) extends Phase[State3, State4] {

  private val astRewriter = new ASTRewriter(sequencer, true)

  override def transform(in: State3, context: Context): State4 = {

    val (rewrittenStatement, extractedParams, postConditions) = astRewriter.rewrite(in.queryText, in.statement, in.semantics)
    in.copy(statement = rewrittenStatement).add(extractedParams, postConditions)
  }

  override def phase = AST_REWRITE

  override def why: String = "Normalizes the AST into a form easier for the planner to work with"
}
