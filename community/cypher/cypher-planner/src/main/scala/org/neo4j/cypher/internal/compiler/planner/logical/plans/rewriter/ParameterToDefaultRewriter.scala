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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.LogicalPlanContainsEagerIfNeeded
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LabelAndRelTypeInfos
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.rewriting.rewriters.parameterRewriter
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.Attributes

case object ParametersRewrittenToDefault extends StepSequencer.Condition

/**
 * Change Parameters in logical plan to default Type and Size.
 */
case object ParameterToDefaultRewriter extends LogicalPlanRewriter with StepSequencer.Step
    with PlanPipelineTransformerFactory {

  override def instance(
    context: PlannerContext,
    solveds: Solveds,
    cardinalities: Cardinalities,
    effectiveCardinalities: EffectiveCardinalities,
    providedOrders: ProvidedOrders,
    labelAndRelTypeInfos: LabelAndRelTypeInfos,
    otherAttributes: Attributes[LogicalPlan],
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    readOnly: Boolean
  ): Rewriter =
    parameterRewriter

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // The rewriters operate on the LogicalPlan
    CompilationContains[LogicalPlan],
    // This should happen after finding the final plan
    LogicalPlanRewritten,
    // This should happen after finding the final plan + Eager changes effective cardinality
    LogicalPlanContainsEagerIfNeeded
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(
    ParametersRewrittenToDefault
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): LogicalPlanRewriter = this
}
