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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.deMorganRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.distributeLawsRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.flattenBooleanOperators
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeInequalities
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeSargablePredicates
import org.neo4j.cypher.internal.rewriting.rewriters.simplifyPredicates
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.inSequence

case object BooleanPredicatesInCNF extends StepSequencer.Condition

/**
 * Normalize boolean predicates into conjunctive normal form.
 */
case object CNFNormalizer extends StatementRewriter with StepSequencer.Step with PlanPipelineTransformerFactory {

  override def instance(context: BaseContext): Rewriter = {
    implicit val monitor: AstRewritingMonitor = context.monitors.newMonitor[AstRewritingMonitor]()
    inSequence(
      deMorganRewriter(),
      distributeLawsRewriter(),
      normalizeInequalities,
      flattenBooleanOperators,
      simplifyPredicates,
      // Redone here since CNF normalization might introduce negated inequalities (which this removes)
      normalizeSargablePredicates
    )
  }

  override def preConditions: Set[StepSequencer.Condition] =
    flattenBooleanOperators.preConditions ++ normalizeSargablePredicates.preConditions

  override def postConditions: Set[StepSequencer.Condition] = Set(BooleanPredicatesInCNF) ++ flattenBooleanOperators.postConditions ++ normalizeSargablePredicates.postConditions

  override def invalidatedConditions: Set[StepSequencer.Condition] =
    normalizeSargablePredicates.invalidatedConditions ++
      flattenBooleanOperators.invalidatedConditions ++
      SemanticInfoAvailable // Introduces new AST nodes

  override def getTransformer(pushdownPropertyReads: Boolean,
                              semanticFeatures: Seq[SemanticFeature]): Transformer[BaseContext, BaseState, BaseState] = this
}
