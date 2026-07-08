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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.rewriting.RewriterStep
import org.neo4j.cypher.internal.rewriting.conditions.ContainsNoExpandableClauses
import org.neo4j.cypher.internal.rewriting.conditions.ContainsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.AddDependenciesToProjectionsInSubqueryExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.AddElementUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.AddQuantifiedPathAnonymousVariableGroupings
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.AddVarLengthBoundPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.CombineSetProperty
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.CypherTypeNormalizationRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.DesugarMapProjection
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.FixedLengthShortestToAllRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.FoldConstants
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.GraphTypeCanonicalizer
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.MoveWithPastMatch
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NameAllPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizeArgumentOrder
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizeComparisons
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizeExistsPatternExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizeNotEquals
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizePredicates
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.ParameterValueTypeReplacement
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.PropertyExistsToIsNotNull
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.QuantifiedPathPatternNodeInsertRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.ReplaceLiteralDynamicPropertyLookups
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.RewriteOrderById
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.RewriteSizeOfCollectToCount
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.SimplifyIterablePredicates
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.UnwrapParenthesizedPath
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.ReturnItemsAreAliased
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

object ASTRewriter {

  val AccumulatedSteps(orderedSteps, postConditions) =
    StepSequencer[StepSequencer.Step with ASTRewriterFactory]().orderSteps(
      Set(
        AddDependenciesToProjectionsInSubqueryExpressions,
        AddQuantifiedPathAnonymousVariableGroupings,
        AddElementUniquenessPredicates,
        AddVarLengthBoundPredicates,
        CombineSetProperty,
        CypherTypeNormalizationRewriter,
        DesugarMapProjection,
        FixedLengthShortestToAllRewriter,
        FoldConstants,
        GraphTypeCanonicalizer,
        LabelExpressionPredicateNormalizer,
        MoveWithPastMatch,
        NameAllPatternElements,
        NormalizeArgumentOrder,
        NormalizeComparisons,
        NormalizeExistsPatternExpressions,
        NormalizeHasLabelsAndHasType,
        NormalizeNotEquals,
        NormalizePredicates,
        ParameterValueTypeReplacement,
        PropertyExistsToIsNotNull,
        QuantifiedPathPatternNodeInsertRewriter,
        ReplaceLiteralDynamicPropertyLookups,
        RewriteOrderById,
        RewriteSizeOfCollectToCount,
        SimplifyIterablePredicates,
        UnwrapParenthesizedPath
      ),
      initialConditions = SemanticInfoAvailable ++ Set(
        ReturnItemsAreAliased,
        ContainsNoExpandableClauses,
        ExpressionsHaveComputedDependencies,
        ContainsNoNodesOfType[PatternComprehension]()
      )
    )

  def rewrite(
    statement: Statement,
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    version: CypherVersion
  ): Statement = {
    val steps = orderedSteps.map { step =>
      val rewriter =
        step.getRewriter(
          semanticState,
          parameterTypeMapping,
          anonymousVariableNameGenerator,
          cancellationChecker,
          version
        )
      (step, RewriterStep.validatingRewriter(rewriter, step, cancellationChecker))
    }

    Transformer.applyRewritersInSequence("AstRewriting", statement, steps)
  }
}
