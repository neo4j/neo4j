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
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.RewriterStep
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.AddQuantifiedPathAnonymousVariableGroupings
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.AddVarLengthPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.CharLengthFunctionRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.FixedLengthShortestToAllRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.QuantifiedPathPatternNodeInsertRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.ReplacePatternComprehensionWithCollectSubquery
import org.neo4j.cypher.internal.rewriting.rewriters.ReturnItemsAreAliased
import org.neo4j.cypher.internal.rewriting.rewriters.RewriteSizeOfCollectToCount
import org.neo4j.cypher.internal.rewriting.rewriters.addDependenciesToProjectionsInSubqueryExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.combineSetProperty
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.rewriting.rewriters.cypherTypeNormalizationRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.desugarMapProjection
import org.neo4j.cypher.internal.rewriting.rewriters.expandStar
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.rewriting.rewriters.foldConstants
import org.neo4j.cypher.internal.rewriting.rewriters.moveWithPastMatch
import org.neo4j.cypher.internal.rewriting.rewriters.nameAllPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeArgumentOrder
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeComparisons
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeExistsPatternExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeNotEquals
import org.neo4j.cypher.internal.rewriting.rewriters.normalizePredicates
import org.neo4j.cypher.internal.rewriting.rewriters.parameterValueTypeReplacement
import org.neo4j.cypher.internal.rewriting.rewriters.replaceLiteralDynamicPropertyLookups
import org.neo4j.cypher.internal.rewriting.rewriters.rewriteOrderById
import org.neo4j.cypher.internal.rewriting.rewriters.simplifyIterablePredicates
import org.neo4j.cypher.internal.rewriting.rewriters.unwrapParenthesizedPath
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

object ASTRewriter {

  val AccumulatedSteps(orderedSteps, postConditions) =
    StepSequencer[StepSequencer.Step with ASTRewriterFactory]().orderSteps(
      Set(
        combineSetProperty,
        expandStar,
        normalizeHasLabelsAndHasType,
        desugarMapProjection,
        moveWithPastMatch,
        normalizeComparisons,
        foldConstants,
        normalizeExistsPatternExpressions,
        nameAllPatternElements,
        normalizePredicates,
        normalizeNotEquals,
        normalizeArgumentOrder,
        AddUniquenessPredicates,
        AddVarLengthPredicates,
        simplifyIterablePredicates,
        replaceLiteralDynamicPropertyLookups,
        parameterValueTypeReplacement,
        rewriteOrderById,
        LabelExpressionPredicateNormalizer,
        unwrapParenthesizedPath,
        QuantifiedPathPatternNodeInsertRewriter,
        addDependenciesToProjectionsInSubqueryExpressions,
        FixedLengthShortestToAllRewriter,
        cypherTypeNormalizationRewriter,
        RewriteSizeOfCollectToCount,
        ReplacePatternComprehensionWithCollectSubquery,
        CharLengthFunctionRewriter,
        AddQuantifiedPathAnonymousVariableGroupings
      ),
      initialConditions = SemanticInfoAvailable ++ Set(
        ReturnItemsAreAliased,
        ExpressionsHaveComputedDependencies
      )
    )

  def rewrite(
    statement: Statement,
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Statement = {
    val rewriters = orderedSteps.map { step =>
      val rewriter =
        step.getRewriter(semanticState, parameterTypeMapping, cypherExceptionFactory, anonymousVariableNameGenerator)
      RewriterStep.validatingRewriter(rewriter, step)
    }

    val combined = inSequence(rewriters: _*)

    statement.endoRewrite(combined)
  }
}
