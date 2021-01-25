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

import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.rewriting.ListStepAccumulator
import org.neo4j.cypher.internal.rewriting.RewriterStep
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralsAreAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.expandCallWhere
import org.neo4j.cypher.internal.rewriting.rewriters.expandShowWhere
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.rewriting.rewriters.insertWithBetweenOptionalMatchAndMatch
import org.neo4j.cypher.internal.rewriting.rewriters.mergeInPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.rewriting.rewriters.replaceDeprecatedCypherSyntax
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.inSequence

case class PreparatoryRewriting(deprecations: Deprecations) extends Phase[BaseContext, BaseState, BaseState] {

  val AccumulatedSteps(orderedSteps, _) = new StepSequencer(ListStepAccumulator[StepSequencer.Step with PreparatoryRewritingRewriterFactory]()).orderSteps(Set(
    normalizeWithAndReturnClauses,
    insertWithBetweenOptionalMatchAndMatch,
    expandCallWhere,
    expandShowWhere,
    replaceDeprecatedCypherSyntax,
    mergeInPredicates), initialConditions = Set(LiteralsAreAvailable))

  override def process(from: BaseState, context: BaseContext): BaseState = {

    val rewriters = orderedSteps.map { step =>
      val rewriter = step.getRewriter(deprecations, context.cypherExceptionFactory, context.notificationLogger)
      RewriterStep.validatingRewriter(rewriter, step)
    }

    val rewrittenStatement = from.statement().endoRewrite(inSequence(rewriters: _*))

    from.withStatement(rewrittenStatement)
  }

  override val phase = AST_REWRITE

  override val description = "rewrite the AST into a shape that semantic analysis can be performed on"

  override def postConditions: Set[StepSequencer.Condition] = Set.empty
}

