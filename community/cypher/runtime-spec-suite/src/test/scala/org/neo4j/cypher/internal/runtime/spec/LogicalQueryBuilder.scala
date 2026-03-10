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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.semantics.CachableSemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedSingleThreaded
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.pos
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder.randomErrorBehaviour
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.attribution.Default
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen

/**
 * Test help utility for hand-writing logical queries.
 */
class LogicalQueryBuilder(
  tokenResolver: Resolver,
  hasLoadCsv: Boolean = false,
  override val idGen: IdGen = new SequentialIdGen(),
  wholePlan: Boolean = true
) extends AbstractLogicalPlanBuilder[LogicalQuery, LogicalQueryBuilder](tokenResolver, wholePlan) {

  private val providedOrders: ProvidedOrders = new ProvidedOrders with Default[LogicalPlan, ProvidedOrder] {
    override val defaultValue: ProvidedOrder = ProvidedOrder.empty
  }

  private val effectiveCardinalities: EffectiveCardinalities =
    new EffectiveCardinalities with Default[LogicalPlan, EffectiveCardinality] {
      override val defaultValue: EffectiveCardinality = EffectiveCardinality(Cardinality.SINGLE.amount)
    }

  private val leveragedOrders: LeveragedOrders = new LeveragedOrders

  private var executionModel: Option[ExecutionModel] = None

  def withProvidedOrder(order: ProvidedOrder): this.type = {
    providedOrders.set(idOfLastPlan, order)
    this
  }

  def withCardinalityEstimation(effectiveCardinality: EffectiveCardinality): this.type = {
    effectiveCardinalities.set(idOfLastPlan, effectiveCardinality)
    this
  }

  def withLeveragedOrder(): this.type = {
    leveragedOrders.set(idOfLastPlan, true)
    this
  }

  def withMorselSize(morselSize: Int): this.type = {
    executionModel = Some(BatchedSingleThreaded(morselSize, morselSize))
    this
  }

  def build(readOnly: Boolean = true): LogicalQuery = {
    val logicalPlan = buildLogicalPlan()
    LogicalQuery(
      logicalPlan.endoRewrite(expressionRewriter),
      "<<queryText>>",
      readOnly,
      resultColumns,
      CachableSemanticTable(semanticTable),
      effectiveCardinalities,
      providedOrders,
      leveragedOrders,
      hasLoadCsv,
      idGen,
      doProfile = false,
      executionPlanCacheKeyHash = 0,
      executionModel = executionModel
    )
  }

  def transactionApplyRandomErrorBehaviour(randomValuesTestSupport: RandomValuesTestSupport[_])(
    batchSize: Int = randomValuesTestSupport.random.between(1, 16),
    maybeReportAs: Option[String] = None
  ): LogicalQueryBuilder = {
    val (errorBehaviour, retryParameters) = randomErrorBehaviour(randomValuesTestSupport, maybeReportAs)
    transactionApply(
      batchSize,
      onErrorBehaviour = errorBehaviour,
      maybeRetryParameters = retryParameters,
      maybeReportAs = maybeReportAs
    )
  }

  def transactionForeachRandomErrorBehaviour(randomValuesTestSupport: RandomValuesTestSupport[_])(
    batchSize: Int = randomValuesTestSupport.random.between(1, 16),
    maybeReportAs: Option[String] = None
  ): LogicalQueryBuilder = {
    val (errorBehaviour, retryParameters) = randomErrorBehaviour(randomValuesTestSupport, maybeReportAs)
    transactionForeach(
      batchSize,
      onErrorBehaviour = errorBehaviour,
      maybeRetryParameters = retryParameters,
      maybeReportAs = maybeReportAs
    )
  }
}

object LogicalQueryBuilder {

  def randomErrorBehaviour(
    randomValuesTestSupport: RandomValuesTestSupport[_],
    maybeReportAs: Option[String] = None
  ): (InTransactionsOnErrorBehaviour, Option[InTransactionsRetryParameters]) = {
    val options = Seq(
      (OnErrorContinue, None),
      (OnErrorBreak, None),
      (OnErrorRetryThenContinue, randomRetryParameters(randomValuesTestSupport)),
      (OnErrorRetryThenBreak, randomRetryParameters(randomValuesTestSupport))
    ) ++ (if (maybeReportAs.isEmpty) Seq(
            // ON ERROR FAIL does not support status report
            (OnErrorFail, None),
            (OnErrorRetryThenFail, randomRetryParameters(randomValuesTestSupport))
          )
          else Seq.empty)
    randomValuesTestSupport.randomAmong(options)
  }

  def randomRetryParameters(randomValuesTestSupport: RandomValuesTestSupport[_])
    : Option[InTransactionsRetryParameters] = {
    val timeout = randomValuesTestSupport.random.between(1.0, 10.0)
    randomValuesTestSupport.randomAmong(Seq(
      None,
      Some(InTransactionsRetryParameters(Some(DecimalDoubleLiteral(timeout.toString)(pos)))(pos))
    ))
  }
}
