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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.StatementRewriter
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.util.StepSequencer

/**
 * Helper trait to embed a rewriter as transformation phase in the scope of the normalisation towards CNF.
 */
trait CnfPhase extends StatementRewriter with StepSequencer.Step
    with PlanPipelineTransformerFactory {
  self: Product =>

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): CnfPhase = this
}

/**
 * Normalize boolean predicates into conjunctive normal form.
 */
object CNFNormalizer {

  val steps: Set[CnfPhase] = {
    Set(
      deMorganRewriter,
      distributeLawsRewriter,
      normalizeInequalities,
      simplifyPredicates,
      normalizeSargablePredicates,
      flattenBooleanOperators
    )
  }

  val PredicatesInCNF: Set[StepSequencer.Condition] = steps.flatMap(_.postConditions)
}
