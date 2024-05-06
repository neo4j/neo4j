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
package org.neo4j.cypher.internal.compiler.planner.logical.debug

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Column
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen

/**
 * Print IR or AST as query result.
 */
case object DebugPrinter extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState] {
  override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    val string =
      if (context.debugOptions.queryGraphEnabled)
        from.query.toString
      else if (context.debugOptions.astEnabled)
        from.statement().toString
      else if (context.debugOptions.semanticStateEnabled)
        from.semantics().toString
      else if (context.debugOptions.logicalPlanEnabled)
        from.logicalPlan.toString
      else if (context.debugOptions.logicalPlanBuilderEnabled)
        LogicalPlanToPlanBuilderString(from.logicalPlan)
      else
        """Output options are: queryGraph, ast, semanticstate, logicalplan, logicalplanbuilder"""

    // We need to do this, otherwise the produced graph statistics won't work for creating an executable plan
    context.planContext.statistics.nodesAllCardinality()

    val (plan, newStatement, newReturnColumns) = stringToLogicalPlan(string)

    // We need to set attributes for the new plan
    copyAttributes(from, plan)

    from.copy(
      maybeLogicalPlan = Some(plan),
      maybeStatement = Some(newStatement),
      maybeReturnColumns = Some(newReturnColumns)
    )
  }

  private def stringToLogicalPlan(string: String): (LogicalPlan, Statement, Seq[String]) = {
    implicit val idGen = new SequentialIdGen()
    val pos = InputPosition(0, 0, 0)
    val stringValues = string.split("\r\n").flatMap(_.split(System.lineSeparator())).flatMap(_.split("\n")).map(s =>
      StringLiteral(s)(pos.withInputLength(0))
    )
    val expression = ListLiteral(stringValues.toSeq)(pos)
    val unwind = UnwindCollection(Argument(Set.empty), varFor("col"), expression)
    val logicalPlan = ProduceResult(unwind, Seq(Column(varFor("col"), Set.empty)))

    val variable = Variable("col")(pos)
    val returnItem = AliasedReturnItem(variable, variable)(pos)
    val returnClause = Return(
      distinct = false,
      ReturnItems(includeExisting = false, Seq(returnItem))(pos),
      None,
      None,
      None,
      Set.empty
    )(pos)
    val newStatement = SingleQuery(Seq(returnClause))(pos)
    val newReturnColumns = Seq("col")

    (logicalPlan, newStatement, newReturnColumns)
  }

  private def copyAttributes(from: LogicalPlanState, plan: LogicalPlan) = {
    from.planningAttributes.solveds.copy(from.logicalPlan.id, plan.id)
    from.planningAttributes.cardinalities.copy(from.maybeLogicalPlan.get.id, plan.id)
    from.planningAttributes.providedOrders.copy(from.logicalPlan.id, plan.id)
    from.planningAttributes.effectiveCardinalities.copy(from.logicalPlan.id, plan.id)
  }

  override def postConditions: Set[StepSequencer.Condition] = Set.empty
}
