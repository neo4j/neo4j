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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.frontend.phases.StatementRewriter
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.util.StepSequencer

trait CnfPhase extends StepSequencer.Step with PlanPipelineTransformerFactory {

  override def invalidatedConditions: Set[StepSequencer.Condition] =
    SemanticInfoAvailable ++ Set(UpToDateScopes, ExpressionsHaveComputedDependencies)
}

/**
 * Helper trait to embed a rewriter as transformation phase in the scope of the normalisation towards CNF.
 */
trait CnfPhaseRewriter extends CnfPhase with StatementRewriter {
  self: Product =>
  override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig): CnfPhaseRewriter = this
  override def invalidatedConditions: Set[StepSequencer.Condition] = super[CnfPhase].invalidatedConditions
}

/**
 * Normalize boolean predicates into conjunctive normal form.
 */
object CNFNormalizer {

  val steps: Set[CnfPhase] = {
    Set(
      deMorganRewriter,
      DistributeLawsRewriterPhase,
      normalizeInequalities,
      simplifyPredicates,
      normalizeSargablePredicates,
      flattenBooleanOperators,
      RemoveRedundantIsNotNullPredicates
    )
  }

  val PredicatesInCNF: Set[StepSequencer.Condition] = steps.flatMap(_.postConditions)
}
