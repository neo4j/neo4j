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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.UsingStatefulShortestPathAll
import org.neo4j.cypher.internal.ast.UsingStatefulShortestPathHint
import org.neo4j.cypher.internal.ast.UsingStatefulShortestPathInto
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.options.CypherStatefulShortestPlanningModeOption
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.topDown

/**
 * Add artificial hints into QueryGraphs with SelectivePathPatterns,
 * depending on the statefulShortestPlanningMode.
 */
case object StatefulShortestPlanningHintsInserter extends PlannerQueryRewriter with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def instance(from: LogicalPlanState, context: PlannerContext): Rewriter = {
    context.statefulShortestPlanningMode match {
      case CypherStatefulShortestPlanningModeOption.intoOnly =>
        topDown(
          rewriter(IntoHinter),
          cancellation = context.cancellationChecker
        )
      case CypherStatefulShortestPlanningModeOption.allIfPossible =>
        topDown(
          rewriter(AllIfPossibleHinter),
          cancellation = context.cancellationChecker
        )
      case CypherStatefulShortestPlanningModeOption.costWeighted =>
        // No hints needed.
        Rewriter.noop
    }
  }

  sealed private trait Hinter {

    def maybeHint(
      arguments: Set[LogicalVariable],
      pathVariables: NonEmptyList[Variable]
    ): Option[UsingStatefulShortestPathHint]
  }

  private case object IntoHinter extends Hinter {

    override def maybeHint(
      arguments: Set[LogicalVariable],
      pathVariables: NonEmptyList[Variable]
    ): Option[UsingStatefulShortestPathHint] =
      Some(UsingStatefulShortestPathInto(pathVariables))
  }

  private case object AllIfPossibleHinter extends Hinter {

    override def maybeHint(
      arguments: Set[LogicalVariable],
      pathVariables: NonEmptyList[Variable]
    ): Option[UsingStatefulShortestPathHint] = {
      val boundaryNodesPreviouslyBound = arguments(pathVariables.head) && arguments(pathVariables.last)
      val sameStartAndEndNode = pathVariables.head == pathVariables.last
      if (boundaryNodesPreviouslyBound || sameStartAndEndNode) {
        // We have to plan Into in these cases
        None
      } else {
        Some(UsingStatefulShortestPathAll(pathVariables))
      }
    }
  }

  private def rewriter(hinter: Hinter): Rewriter =
    Rewriter.lift {
      case qg: QueryGraph =>
        val hints = qg.selectivePathPatterns.flatMap {
          ssp =>
            val vars = NonEmptyList.from(ssp.pathVariables.map(_.variable.asInstanceOf[Variable]))
            hinter.maybeHint(qg.argumentIds.map(varFor), vars)
        }
        qg.addHints(hints)
    }

  // we just want to have the planner query created
  override def preConditions: Set[StepSequencer.Condition] =
    Set(CompilationContains[PlannerQuery]()) // = CreatePlannerQuery.postConditions

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[_ <: BaseContext, _ <: BaseState, BaseState] = this
}
