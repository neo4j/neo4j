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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.RewriterStep
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.GQLAliasFunctionNameRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.ExpandShowWhere
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.MergeInPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.NormalizeWithAndReturnClauses
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.NullIfFunctionRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.RemoveSyntaxTracking
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.RewriteGraphTypeReferences
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.RewriteShortestPathWithFixedLengthRelationship
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.RewriteShowQuery
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.SearchPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.TimestampRewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps

/**
 * Rewrite the AST into a shape that semantic analysis can be performed on.
 */
case object PreparatoryRewriting extends Phase[BaseContext, BaseState, BaseState] with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  val AccumulatedSteps(orderedSteps, _postConditions) =
    StepSequencer[StepSequencer.Step with PreparatoryRewritingRewriterFactory]().orderSteps(
      Set(
        ExpandShowWhere,
        GQLAliasFunctionNameRewriter,
        MergeInPredicates,
        NormalizeWithAndReturnClauses,
        NullIfFunctionRewriter,
        RemoveSyntaxTracking,
        TimestampRewriter,
        RewriteGraphTypeReferences,
        RewriteShortestPathWithFixedLengthRelationship,
        RewriteShowQuery,
        SearchPredicateNormalizer
      )
    )

  override def process(from: BaseState, context: BaseContext): BaseState = {

    val steps = orderedSteps.map { step =>
      val rewriter = step.getRewriter(context.cypherExceptionFactory, Some(context.cypherVersion))
      (step, RewriterStep.validatingRewriter(rewriter, step, context.cancellationChecker))
    }

    from.withStatement(Transformer.applyRewritersInSequence("PreparatoryRewriting", from.statement(), steps))
  }

  override val phase = AST_REWRITE

  case object SemanticAnalysisPossible extends StepSequencer.Condition

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[Statement]())

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(UpToDateScopes)

  override def postConditions: Set[StepSequencer.Condition] = _postConditions + SemanticAnalysisPossible

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this
}
