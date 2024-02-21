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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.defaultUpdateStrategy
import org.neo4j.cypher.internal.compiler.eagerUpdateStrategy
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CompressPlanIDs
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId

import scala.collection.immutable.ListSet

case object LogicalPlanContainsEagerIfNeeded extends StepSequencer.Condition

/**
 * [[EagernessReason.Conflict]] contains references to other plans by ID.
 * This condition is important so that other rewriters that change and create
 * IDs can be run before this phase, or take extra actions to make sure that
 * references are updates accordingly.
 */
case object LogicalPlanContainsIDReferences extends StepSequencer.Condition

/**
 * Insert Eager into the logical plan where needed.
 */
case object EagerRewriter extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState] with StepSequencer.Step
    with PlanPipelineTransformerFactory {

  override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    if (context.eagerAnalyzer != CypherEagerAnalyzerOption.lp) return from
    if (from.logicalPlan.readOnly) return from

    val attributes: Attributes[LogicalPlan] = from.planningAttributes.asAttributes(context.logicalPlanIdGen)
    val lPStateWithEagerProcedureCall = eagerizeProcedureCalls(from, attributes)

    val cardinalities = lPStateWithEagerProcedureCall.planningAttributes.cardinalities

    val newPlan = context.updateStrategy match {
      case `eagerUpdateStrategy` => EagerEverywhereRewriter(attributes).eagerize(
          lPStateWithEagerProcedureCall.logicalPlan,
          lPStateWithEagerProcedureCall.semanticTable(),
          lPStateWithEagerProcedureCall.anonymousVariableNameGenerator
        )
      case `defaultUpdateStrategy` =>
        val shouldCompressReasons = !context.debugOptions.verboseEagernessReasons
        EagerWhereNeededRewriter(cardinalities, attributes, shouldCompressReasons).eagerize(
          lPStateWithEagerProcedureCall.logicalPlan,
          lPStateWithEagerProcedureCall.semanticTable(),
          lPStateWithEagerProcedureCall.anonymousVariableNameGenerator
        )
    }

    lPStateWithEagerProcedureCall.withMaybeLogicalPlan(Some(newPlan))
  }

  private def eagerizeProcedureCalls(from: LogicalPlanState, attributes: Attributes[LogicalPlan]): LogicalPlanState =
    from.withMaybeLogicalPlan(Some(
      EagerProcedureCallRewriter(attributes).eagerize(
        from.logicalPlan,
        from.semanticTable(),
        from.anonymousVariableNameGenerator
      )
    ))

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // The rewriter operates on the LogicalPlan
    CompilationContains[LogicalPlan]()
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(
    LogicalPlanContainsEagerIfNeeded,
    LogicalPlanContainsIDReferences
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // Rewriting logical plans introduces new IDs
    CompressPlanIDs.completed
  )

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this
}

abstract class EagerRewriter(attributes: Attributes[LogicalPlan]) {

  /**
   * Inserts Eager on top of the given plan. If the given plan is already Eager, merges the Eagerness reasons.
   */
  protected def eagerOnTopOf(plan: LogicalPlan, reasons: ListSet[EagernessReason]): Eager = {
    plan match {
      case eager @ Eager(innerPlan, moreReasons) => Eager(innerPlan, reasons ++ moreReasons)(SameId(eager.id))
      case _                                     => Eager(plan, reasons)(attributes.copy(plan.id))
    }
  }

  /**
   * Insert Eager at least everywhere it's needed to maintain correct semantics.
   *
   * @param plan the whole logical plan
   * @return the rewritten logical plan
   */
  def eagerize(
    plan: LogicalPlan,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): LogicalPlan
}
