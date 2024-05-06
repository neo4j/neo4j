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
package org.neo4j.cypher.internal.runtime.spec.rewriters

import org.neo4j.cypher.internal.expressions.CollectAll
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.runtime.spec.rewriters.PlanRewriterContext.pos
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.NoEager
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriterConfig.PlanRewriterStepConfig
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanRewriterTemplates.onlyRewriteLogicalPlansStopper
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanRewriterTemplates.randomShouldApply
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.noop
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * Rewrite
 *
 *    ProduceResult
 *    LHS
 *
 * to
 *
 *    ProduceResult
 *    Unwind
 *    Aggregation(Collect)
 *    LHS
 *
 */
case class CollectUnwindOnTop(
  ctx: PlanRewriterContext,
  config: PlanRewriterStepConfig
) extends Rewriter {

  private val instance: Rewriter =
    if (ctx.config.hints.contains(NoEager)) noop
    else topDown(
      Rewriter.lift {
        case pr @ ProduceResult(source, columns)
          if !ctx.leveragedOrders(source.id) && randomShouldApply(config) =>
          val collectedRowsName = ctx.anonymousVariableNameGenerator.nextName
          val collectedRowsVar = Variable(collectedRowsName)(pos)
          val unwoundRowName = ctx.anonymousVariableNameGenerator.nextName
          val unwoundRowVar = Variable(unwoundRowName)(pos)
          val rowMapExpr = MapExpression(columns.map { c => PropertyKeyName(c.variable.name)(pos) -> c.variable })(pos)
          val collectExpr = CollectAll(rowMapExpr)(pos)
          val aggregation = Aggregation(source, Map.empty, Map(varFor(collectedRowsName) -> collectExpr))(ctx.idGen)
          val unwind = UnwindCollection(aggregation, varFor(unwoundRowName), collectedRowsVar)(ctx.idGen)
          val projections = columns.map { c =>
            c.variable -> Property(unwoundRowVar, PropertyKeyName(c.variable.name)(pos))(pos)
          }.toMap
          val project = Projection(unwind, projections)(ctx.idGen)
          ProduceResult(project, columns)(SameId(pr.id))
      },
      onlyRewriteLogicalPlansStopper
    )

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
