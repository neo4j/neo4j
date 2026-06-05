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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.LogicalPlanRewritten
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TransactionDisjointByRewriter
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.options.CypherTransactionBatchStrategyOption
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition

/**
 * Identifies plans that can utilize disjointBy in TransactionApply or TransactionForeach
 */
case object TransactionDisjointBy extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState]
    with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING

  override def preConditions: Set[StepSequencer.Condition] = Set(
    LogicalPlanRewritten
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig)
    : Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    val globalStrategyIsAuto = context.transactionBatchStrategy eq CypherTransactionBatchStrategyOption.auto
    val cypherVersionHasSyntax = context.cypherVersion != CypherVersion.Cypher5
    if (globalStrategyIsAuto || cypherVersionHasSyntax) {
      from.withMaybeLogicalPlan(
        Some(from.logicalPlan.endoRewrite(TransactionDisjointByRewriter(globalStrategyIsAuto)))
      )
    } else {
      from
    }
  }
}
