/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.ast.NotEquals
import org.neo4j.cypher.internal.frontend.v3_2.ast.conditions._
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.{ASTRewriter, LiteralExtraction}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE

case class AstRewriting(sequencer: String => RewriterStepSequencer, literalExtraction: LiteralExtraction) extends Phase[BaseContext, BaseState, BaseState] {

  private val astRewriter = new ASTRewriter(sequencer, literalExtraction)

  override def process(in: BaseState, context: BaseContext): BaseState = {

    val (rewrittenStatement, extractedParams, _) = astRewriter.rewrite(in.queryText, in.statement(), in.semantics())

    in.withStatement(rewrittenStatement).withParams(extractedParams)
  }

  override def phase = AST_REWRITE

  override def description = "normalize the AST into a form easier for the planner to work with"

  override def postConditions: Set[Condition] = {
    val rewriterConditions = Set(
      noReferenceEqualityAmongVariables,
      orderByOnlyOnVariables,
      noDuplicatesInReturnItems,
      containsNoReturnAll,
      noUnnamedPatternElementsInMatch,
      containsNoNodesOfType[NotEquals],
      normalizedEqualsArguments,
      aggregationsAreIsolated,
      noUnnamedPatternElementsInPatternComprehension
    )

    rewriterConditions.map(StatementCondition.apply)
  }
}
