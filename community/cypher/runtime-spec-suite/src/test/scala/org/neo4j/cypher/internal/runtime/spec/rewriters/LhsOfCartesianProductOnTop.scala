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

import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriterConfig.PlanRewriterStepConfig
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanRewriterTemplates.onlyRewriteLogicalPlansStopper
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanRewriterTemplates.randomShouldApply
import org.neo4j.cypher.internal.util.Rewriter
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
 *    CartesianProduct
 *    LHS  Argument
 *
 */
case class LhsOfCartesianProductOnTop(
  ctx: PlanRewriterContext,
  config: PlanRewriterStepConfig
) extends Rewriter {

  private val instance: Rewriter = topDown(
    Rewriter.lift {
      case pr @ ProduceResult(source, columns, cachedProperties) if randomShouldApply(config) =>
        val argument = Argument()(ctx.idGen)
        val cartesianProduct = CartesianProduct(source, argument)(ctx.idGen)
        ProduceResult(cartesianProduct, columns, cachedProperties)(SameId(pr.id))
    },
    onlyRewriteLogicalPlansStopper
  )

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}

case class LhsOfCartesianProductOnTopNoUpdatingRhs(
  ctx: PlanRewriterContext,
  config: PlanRewriterStepConfig
) extends Rewriter {

  private val instance: Rewriter = topDown(
    Rewriter.lift {
      case pr @ ProduceResult(source, columns, _) if randomShouldApply(config) =>
        val argument = Argument()(ctx.idGen)
        val cartesianProduct = CartesianProduct(source, argument)(ctx.idGen)
        if (cartesianProduct.hasUpdatingRhs) pr
        else ProduceResult(cartesianProduct, columns)(SameId(pr.id))
    },
    onlyRewriteLogicalPlansStopper
  )

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
