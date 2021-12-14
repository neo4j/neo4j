/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.mergeDuplicateBooleanOperators
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.normalizeSargablePredicates
import org.neo4j.cypher.internal.rewriting.ListStepAccumulator
import org.neo4j.cypher.internal.rewriting.RewriterStep
import org.neo4j.cypher.internal.rewriting.conditions.PatternExpressionsHaveSemanticInfo
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.ProjectionClausesHaveSemanticInfo
import org.neo4j.cypher.internal.rewriting.rewriters.desugarMapProjection
import org.neo4j.cypher.internal.rewriting.rewriters.expandStar
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.rewriting.rewriters.foldConstants
import org.neo4j.cypher.internal.rewriting.rewriters.inlineNamedPathsInPatternComprehensions
import org.neo4j.cypher.internal.rewriting.rewriters.moveWithPastMatch
import org.neo4j.cypher.internal.rewriting.rewriters.nameAllPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeArgumentOrder
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeComparisons
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeExistsPatternExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeMatchPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeNotEquals
import org.neo4j.cypher.internal.rewriting.rewriters.parameterValueTypeReplacement
import org.neo4j.cypher.internal.rewriting.rewriters.replaceLiteralDynamicPropertyLookups
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.CypherType

object ASTRewriter {

  private val AccumulatedSteps(orderedSteps, _) = StepSequencer(ListStepAccumulator[StepSequencer.Step with ASTRewriterFactory]()).orderSteps(Set(
    expandStar,
    normalizeHasLabelsAndHasType,
    desugarMapProjection,
    moveWithPastMatch,
    normalizeComparisons,
    foldConstants,
    mergeDuplicateBooleanOperators,
    normalizeExistsPatternExpressions,
    nameAllPatternElements,
    normalizeMatchPredicates,
    normalizeNotEquals,
    normalizeArgumentOrder,
    normalizeSargablePredicates,
    AddUniquenessPredicates,
    replaceLiteralDynamicPropertyLookups,
    inlineNamedPathsInPatternComprehensions,
    parameterValueTypeReplacement,
  ), initialConditions = Set(ProjectionClausesHaveSemanticInfo, PatternExpressionsHaveSemanticInfo))

  def rewrite(statement: Statement,
              semanticState: SemanticState,
              parameterTypeMapping: Map[String, CypherType],
              cypherExceptionFactory: CypherExceptionFactory,
              anonymousVariableNameGenerator: AnonymousVariableNameGenerator
             ): Statement = {
    val rewriters = orderedSteps.map { step =>
      val rewriter = step.getRewriter(semanticState, parameterTypeMapping, cypherExceptionFactory, anonymousVariableNameGenerator)
      RewriterStep.validatingRewriter(rewriter, step)
    }

    val combined = inSequence(rewriters: _*)

    statement.endoRewrite(combined)
  }
}
