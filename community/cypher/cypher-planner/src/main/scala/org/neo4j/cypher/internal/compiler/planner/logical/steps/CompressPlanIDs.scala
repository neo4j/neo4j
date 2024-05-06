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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.topDown

/**
 * Compresses the plan IDs so that they are consecutive numbers starting from 0.
 * Create a copy of the planning attributes with the new IDs.
 *
 * This is helpful for physical planning attributes that do not have to create so large arrays.
 * It also reduces the size of what we need to put into the query cache.
 */
case object CompressPlanIDs extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState]
    with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    val oldAttributes = from.planningAttributes
    val newAttributes = PlanningAttributes.newAttributes

    val fixIdReferences = new FixIdReferences(context.cancellationChecker)

    val newIdGen = new SequentialIdGen()
    val newPlan = from.logicalPlan.endoRewrite(topDown(
      rewriter = Rewriter.lift {
        case lp: LogicalPlan =>
          val newLP = lp.copyPlanWithIdGen(newIdGen)
          fixIdReferences.registerMapping(lp.id, newLP.id)

          oldAttributes.solveds.getOption(lp.id).foreach {
            newAttributes.solveds.set(newLP.id, _)
          }
          oldAttributes.cardinalities.getOption(lp.id).foreach {
            newAttributes.cardinalities.set(newLP.id, _)
          }
          oldAttributes.effectiveCardinalities.getOption(lp.id).foreach {
            newAttributes.effectiveCardinalities.set(newLP.id, _)
          }
          oldAttributes.providedOrders.getOption(lp.id).foreach {
            newAttributes.providedOrders.set(newLP.id, _)
          }
          oldAttributes.leveragedOrders.getOption(lp.id).foreach {
            newAttributes.leveragedOrders.set(newLP.id, _)
          }
          oldAttributes.labelAndRelTypeInfos.getOption(lp.id).foreach {
            newAttributes.labelAndRelTypeInfos.set(newLP.id, _)
          }
          newLP
      },
      leftToRight = false,
      cancellation = context.cancellationChecker
    ))

    val newPlanWithUpdatedIDRefs = newPlan.endoRewrite(fixIdReferences(recursiveIdLookup = false))

    from
      .withMaybeLogicalPlan(Some(newPlanWithUpdatedIDRefs))
      .withNewPlanningAttributes(newAttributes)
  }

  override def name: String = "CompressPlanIDs"

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // Traverses the logical plan
    CompilationContains[LogicalPlan]()
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this
}
