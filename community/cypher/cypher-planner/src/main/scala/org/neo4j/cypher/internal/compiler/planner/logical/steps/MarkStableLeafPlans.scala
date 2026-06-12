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

import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.StableLeafPlan
import org.neo4j.cypher.internal.planner.spi.LeafStability
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition

/**
 * Marks leaf plans that are stable iterators: leaf scans guaranteed to be initialized before any write in the
 * query, on an MVCC storage format, when the transaction state is empty.
 */
case object MarkStableLeafPlans extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState]
    with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    if (context.planContext.storageIsMvcc && !from.logicalPlan.readOnly) {
      val stability =
        if (context.planContext.txStateHasChanges()) LeafStability.MvccNonEmptyTx
        else LeafStability.MvccEmptyTx
      stableLeaves(from.logicalPlan).foreach { leaf =>
        from.planningAttributes.stableLeafPlans.set(leaf.id, stability)
      }
    }
    from
  }

  private def stableLeaves(plan: LogicalPlan): Option[LogicalPlan] =
    Some(plan.leftmostLeaf).filter(_.isInstanceOf[StableLeafPlan])

  // This phase must run before EagerRewriter so eagerness analysis can use the classification. That ordering
  // is enforced by EagerRewriter.preConditions requiring MarkStableLeafPlans.completed. We deliberately do
  // NOT depend on CompressPlanIDs here: the attribute is keyed by plan id and CompressPlanIDs remaps it like
  // any other attribute, so the marking is correct regardless of where it lands relative to id compression.
  override def preConditions: Set[StepSequencer.Condition] = Set(
    CompilationContains[LogicalPlan]()
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig)
    : Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this
}
