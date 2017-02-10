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
package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.compiler.v3_2.ASTRewriter
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.compiler.v3_2.ast.conditions._
import org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_2.ast.NotEquals
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.{RewriterCondition, RewriterStepSequencer}
import org.neo4j.cypher.internal.frontend.v3_2.phases.BaseContext
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, inSequence}

case class AstRewriting(sequencer: String => RewriterStepSequencer, shouldExtractParams: Boolean) extends Phase[BaseContext, BaseState, BaseState] {

  private val astRewriter = new ASTRewriter(sequencer, shouldExtractParams)

  override def process(in: BaseState, context: BaseContext): BaseState = {

    val (rewrittenStatement, extractedParams, _) = astRewriter.rewrite(in.queryText, in.statement(), in.semantics())

    CompilationState(in).copy(
      maybeStatement = Some(rewrittenStatement),
      maybeExtractedParams = Some(extractedParams))
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

object LateAstRewriting extends StatementRewriter {
  override def instance(context: BaseContext): Rewriter = inSequence(
    collapseMultipleInPredicates,
    nameUpdatingClauses,
    projectNamedPaths,
//    enableCondition(containsNamedPathOnlyForShortestPath), // TODO Re-enable
    projectFreshSortExpressions
  )

  override def description: String = "normalize the AST"

  override def postConditions: Set[Condition] = Set.empty
}

object StatementCondition {
  def apply(inner: RewriterCondition) = new StatementCondition(inner.condition)
}

case class StatementCondition(inner: Any => Seq[String]) extends Condition {
  override def check(state: CompilationState): Seq[String] = inner(state.statement)
}
