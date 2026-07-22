/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UseAsMultipleGraphsSelector
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.VisitorPhase
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.values.virtual.MapValue

trait GraphTargetVerifier {

  def verifyGraphTarget(
    statement: Statement,
    databaseId: NamedDatabaseId,
    allowCompositeQueries: Boolean,
    params: MapValue
  ): Unit
}

/**
 * Verifies correct graph selection done with USE clause.
 * Query router performs graph selection evaluation and sends a query
 * to a correct target, so this check is here mainly for queries submitted through
 * Core API which do not go through Query router.
 * USE clause is allowed for Core API queries, but since no routing is performed for such queries,
 * the USE clause is permitted to evaluate only to the session graph.
 * This verifier performs check for combination of explicit and ambient graph selection which is
 * useful even for queries that have gone through Query router as this check is not (and cannot be)
 * performed by semantic analysis.
 */
case object VerifyGraphTarget extends VisitorPhase[PlannerContext, BaseState] with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def phase: CompilationPhaseTracer.CompilationPhase = CompilationPhase.LOGICAL_PLANNING

  override def visit(value: BaseState, context: PlannerContext): Unit = {
    // We skip this check when the new stack is enabled and we are targeting a composite DB
    if (!context.semanticFeatures.contains(UseAsMultipleGraphsSelector)) {
      context.graphTargetVerifier.verifyGraphTarget(
        value.statement(),
        context.databaseId,
        context.config.queryRouterForCompositeQueriesEnabled,
        context.params
      )
    }
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[Statement]())

  // necessary because VisitorPhase defines empty postConditions
  override def postConditions: Set[StepSequencer.Condition] = Set(completed)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig)
    : VisitorPhase[PlannerContext, BaseState] = this

}
