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
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator.WithState
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.attribution.Default
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.NullLog
import org.scalacheck.Gen

import scala.jdk.CollectionConverters.IteratorHasAsScala

object LogicalQueryGenerator {

  def logicalQuery(
    txContext: TransactionalContext,
    costLimit: Cost,
    nodes: Seq[Node],
    rels: Seq[Relationship]
  ): Gen[WithState[(LogicalQuery, PlanContext)]] = {
    val providedOrders: ProvidedOrders = new ProvidedOrders with Default[LogicalPlan, ProvidedOrder] {
      override val defaultValue: ProvidedOrder = ProvidedOrder.empty
    }
    val leveragedOrders = new LeveragedOrders

    val tokenRead = txContext.kernelTransaction().tokenRead()
    val log = NullLog.getInstance()
    val planContext = TransactionBoundPlanContext(TransactionalContextWrapper(txContext), devNullLogger, log)
    val labelMap = tokenRead.labelsGetAllTokens().asScala.map(l => l.name() -> l.id()).toMap
    val relMap = tokenRead.relationshipTypesGetAllTokens().asScala.toVector.map(r => r.name() -> r.id()).toMap

    for {
      WithState(logicalPlan, state) <-
        new LogicalPlanGenerator(labelMap, relMap, planContext, costLimit, nodes, rels).logicalPlan
    } yield {

      val effectiveCardinalities = new EffectiveCardinalities
      state.cardinalities.iterator.foreach(cp => effectiveCardinalities.set(cp._1, EffectiveCardinality(cp._2.amount)))

      WithState(
        (
          LogicalQuery(
            logicalPlan,
            "<<queryText>>",
            readOnly = true,
            logicalPlan.availableSymbols.map(_.name).toArray,
            state.semanticTable,
            effectiveCardinalities,
            providedOrders,
            leveragedOrders,
            hasLoadCSV = false,
            state.idGen,
            doProfile = false
          ),
          planContext
        ),
        state
      )
    }
  }
}
