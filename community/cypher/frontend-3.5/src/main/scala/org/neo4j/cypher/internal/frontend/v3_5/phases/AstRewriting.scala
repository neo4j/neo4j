/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_5.phases

import org.neo4j.cypher.internal.frontend.v3_5.ast.conditions._
import org.neo4j.cypher.internal.frontend.v3_5.ast.rewriters.{ASTRewriter, LiteralExtraction}
import org.neo4j.cypher.internal.frontend.v3_5.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_5.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.v3_5.expressions.NotEquals

case class AstRewriting(sequencer: String => RewriterStepSequencer, literalExtraction: LiteralExtraction,
                        getDegreeRewriting: Boolean = true// This does not really belong in the front end. Should move to a planner rewriter
) extends Phase[BaseContext, BaseState, BaseState] {

  private val astRewriter = new ASTRewriter(sequencer, literalExtraction, getDegreeRewriting)

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
