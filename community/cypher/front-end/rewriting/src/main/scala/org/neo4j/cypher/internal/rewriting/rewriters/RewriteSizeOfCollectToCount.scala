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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.rewriting.conditions.SizeOfCollectRewrittenToCount
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * Rewrites 
 * {{{
 * size(COLLECT { MATCH (a) RETURN a })
 * }}}
 * to
 * {{{
 * COUNT { MATCH (a) RETURN a }
 * }}}
 */
case object RewriteSizeOfCollectToCount extends StepSequencer.Step with ASTRewriterFactory {

  override def preConditions: Set[Condition] = Set(
    // Pattern comprehensions must have been rewritten to COLLECT
    containsNoNodesOfType[PatternComprehension]()
  )

  override def postConditions: Set[Condition] = Set(SizeOfCollectRewrittenToCount)

  override def invalidatedConditions: Set[Condition] = Set(
    // It can invalidate this condition by rewriting things inside WITH/RETURN.
    ProjectionClausesHaveSemanticInfo
  )

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = instance

  val instance: Rewriter = bottomUp(Rewriter.lift {
    case Size(ce: CollectExpression) =>
      CountExpression(ce.query)(ce.position, ce.computedIntroducedVariables, ce.computedScopeDependencies)
  })

}
