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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.StatementRewriter
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.inSequence

/**
 * Helper trait to embed a rewriter as transformation phase in the scope of the normalisation towards CNF.
 */
trait CnfPhase extends Phase[BaseContext, BaseState, BaseState] with StepSequencer.Step with PlanPipelineTransformerFactory {
  self: Product =>

  override def getTransformer(pushdownPropertyReads: Boolean,
                              semanticFeatures: Seq[SemanticFeature]): CnfPhase with Product = this

  override def phase: CompilationPhase = CompilationPhase.AST_REWRITE

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val rewritten = from.statement().endoRewrite(getRewriter(from, context))
    from.withStatement(rewritten)
  }

  def getRewriter(from: BaseState, context: BaseContext) : Rewriter
}

/**
 * Normalize boolean predicates into conjunctive normal form.
 */
case object CNFNormalizer extends StatementRewriter {
  /* These implementations are here to make org.neo4j.cypher.internal.frontend.phases.CompilationPhases compile and for tests to pass.
   * Note, that this current implementation is missing the simplifyPredicates step.
   */
  override def instance(from: BaseState, context: BaseContext): Rewriter = {
    implicit val monitor: AstRewritingMonitor = context.monitors.newMonitor[AstRewritingMonitor]()
    inSequence(
      deMorganRewriter(),
      distributeLawsRewriter(),
      normalizeInequalities,
      flattenBooleanOperators,
      // Redone here since CNF normalization might introduce negated inequalities (which this removes)
      normalizeSargablePredicates
    )
  }

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  val steps: Set[StepSequencer.Step with PlanPipelineTransformerFactory] = {
    Set(
      deMorganRewriter,
      distributeLawsRewriter,
      normalizeInequalities,
      simplifyPredicates,
      normalizeSargablePredicates,
      flattenBooleanOperators)
  }

  val PredicatesInCNF: Set[StepSequencer.Condition] = steps.flatMap(_.postConditions)
}

case object NotsBelowBooleanOperators extends StepSequencer.Condition

case object NoXorOperators extends StepSequencer.Condition

case object AndsAboveOrs extends StepSequencer.Condition

case object NoDuplicateNeighbouringBooleanOperands extends StepSequencer.Condition

case object AndRewrittenToAnds extends StepSequencer.Condition

case object PredicatesSimplified extends StepSequencer.Condition

case object NoInequalityInsideNot extends StepSequencer.Condition
