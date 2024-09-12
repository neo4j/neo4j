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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.RewriterStep
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.rewriting.rewriters.expandCallWhere
import org.neo4j.cypher.internal.rewriting.rewriters.expandShowWhere
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.rewriting.rewriters.insertWithBetweenOptionalMatchAndMatch
import org.neo4j.cypher.internal.rewriting.rewriters.mergeInPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.rewriting.rewriters.nullIfFunctionRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.rewriteShortestPathWithFixedLengthRelationship
import org.neo4j.cypher.internal.rewriting.rewriters.rewriteShowQuery
import org.neo4j.cypher.internal.rewriting.rewriters.timestampRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.wrapOptionalCallProcedure
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * Rewrite the AST into a shape that semantic analysis can be performed on.
 */
case object PreparatoryRewriting extends Phase[BaseContext, BaseState, BaseState] with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  val AccumulatedSteps(orderedSteps, _postConditions) =
    StepSequencer[StepSequencer.Step with PreparatoryRewritingRewriterFactory]().orderSteps(
      Set(
        normalizeWithAndReturnClauses,
        insertWithBetweenOptionalMatchAndMatch,
        wrapOptionalCallProcedure,
        expandCallWhere,
        expandShowWhere,
        rewriteShowQuery,
        mergeInPredicates,
        timestampRewriter,
        rewriteShortestPathWithFixedLengthRelationship,
        nullIfFunctionRewriter
      )
    )

  override def process(from: BaseState, context: BaseContext): BaseState = {

    val rewriters = orderedSteps.map { step =>
      val rewriter = step.getRewriter(context.cypherExceptionFactory)
      RewriterStep.validatingRewriter(rewriter, step, context.cancellationChecker)
    }

    val rewrittenStatement = from.statement().endoRewrite(inSequence(rewriters.toSeq: _*))

    from.withStatement(rewrittenStatement)
  }

  override val phase = AST_REWRITE

  case object SemanticAnalysisPossible extends StepSequencer.Condition

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[Statement]())

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = _postConditions + SemanticAnalysisPossible

  override def getTransformer(
    literalExtractionStrategy: LiteralExtractionStrategy,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    semanticFeatures: Seq[SemanticFeature],
    obfuscateLiterals: Boolean
  ): Transformer[BaseContext, BaseState, BaseState] = this
}
