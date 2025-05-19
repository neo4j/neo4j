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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.ast.semantics.CachableSemanticTable
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ApplyPlans
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.LiveVariables
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.TrailPlans
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.Result
import org.neo4j.cypher.internal.runtime.slottedParameters
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.attribution.ImmutableAttribute

object PhysicalPlanner {

  def plan(
    tokenContext: ReadTokenContext,
    beforeRewrite: LogicalPlan,
    semanticTable: CachableSemanticTable,
    breakingPolicy: PipelineBreakingPolicy,
    config: CypherRuntimeConfiguration,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    allocatePipelinedSlots: Boolean = false
  ): PhysicalPlan = {
    DebugSupport.PHYSICAL_PLANNING.log(
      "======== BEGIN Physical Planning with %-31s ===========================",
      breakingPolicy.getClass.getSimpleName
    )
    val Result(logicalPlan, nExpressionSlots, availableExpressionVars) =
      expressionVariableAllocation.allocate(beforeRewrite)
    val (withSlottedParameters, parameterMapping) = slottedParameters(logicalPlan)
    val liveVariables =
      if (config.freeMemoryOfUnusedColumns) LivenessAnalysis.computeLiveVariables(logicalPlan, breakingPolicy)
      else new LiveVariables
    val slotMetaData = SlotAllocation.allocateSlots(
      withSlottedParameters,
      semanticTable,
      breakingPolicy,
      availableExpressionVars,
      config,
      anonymousVariableNameGenerator,
      liveVariables,
      cancellationChecker,
      allocatePipelinedSlots
    )
    val finalLogicalPlan = new SlottedRewriter(tokenContext)
      .apply(withSlottedParameters, slotMetaData.slotConfigurations, slotMetaData.trailPlans)
    DebugSupport.PHYSICAL_PLANNING.log(
      "======== END Physical Planning =================================================================="
    )
    PhysicalPlan(
      finalLogicalPlan,
      nExpressionSlots,
      slotMetaData.slotConfigurations.finalizeSlots(),
      slotMetaData.argumentSizes,
      slotMetaData.applyPlans,
      slotMetaData.trailPlans,
      slotMetaData.nestedPlanArgumentConfigurations.finalizeSlots(),
      availableExpressionVars,
      parameterMapping
    )
  }
}

case class PhysicalPlan(
  logicalPlan: LogicalPlan,
  nExpressionSlots: Int,
  slotConfigurations: ImmutableAttribute[SlotConfiguration],
  argumentSizes: ArgumentSizes,
  applyPlans: ApplyPlans,
  trailPlans: TrailPlans,
  nestedPlanArgumentConfigurations: ImmutableAttribute[SlotConfiguration],
  availableExpressionVariables: AvailableExpressionVariables,
  parameterMapping: ParameterMapping
)
