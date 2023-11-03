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

import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator

/**
 * On the way from query to plan, we generate a number of anonymous variables. Not all of them are used in the final plan
 * and sometimes we generate some for candidates that are discarded at a later stage.
 * Therefore, the sequence of anonymous variables may be discontinuous, which this rewriter tries to fix, so that we get
 * reliable anonymous variable names in the tests.
 *
 * While it would be a lot safer to work on Variables, the rewriter works on Strings, as variable names may appear in
 * Strings such as `NestedPlanExpression#solvedExpressionAsString` where. This should therefore not be used in
 * production but should be fine for use in tests.
 */
case object CompressAnonymousVariables extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState] {

  override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState =
    from
      .withMaybeLogicalPlan(from.maybeLogicalPlan.map(apply))

  def apply[T <: AnyRef](input: T): T = {
    val anonymousVariables = input.folder.treeCollect {
      case v @ LogicalVariable(NameDeduplicator.UNNAMED_PATTERN(_, _)) => v
    }

    val compression = anonymousVariables
      .distinct
      // we need to sort because some plan constructs contain anonymous variables in sets which are otherwise non-deterministic in their order
      .sortBy {
        case LogicalVariable(NameDeduplicator.UNNAMED_PATTERN(_, index)) => index.toInt
        case _                                                           => throw new IllegalStateException()
      }
      .zipWithIndex
      .map {
        case (key, value) =>
          s"${key.name}\\b" -> AnonymousVariableNameGenerator.anonymousVarName(value)
      }

    val rewriter = bottomUp(Rewriter.lift {
      case s: String =>
        // We need string replacement to fix up solved expression string
        compression.foldLeft(s)((s, replacement) => s.replaceAll(replacement._1, replacement._2))
    })

    rewriter.apply(input).asInstanceOf[T]
  }

  override def postConditions: Set[StepSequencer.Condition] = Set.empty
}
