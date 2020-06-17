/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator.WithState
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.spi.TransactionBoundGraphStatistics
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.attribution.Default
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.NullLog
import org.scalacheck.Gen

import scala.collection.JavaConverters.asScalaIteratorConverter

object LogicalQueryGenerator {

  def logicalQuery(txContext: TransactionalContext, costLimit: Cost, nodes: Seq[Node], rels: Seq[Relationship]): Gen[WithState[LogicalQuery]] = {
    val providedOrders: ProvidedOrders = new ProvidedOrders with Default[LogicalPlan, ProvidedOrder] {
      override val defaultValue: ProvidedOrder = ProvidedOrder.empty
    }
    val leveragedOrders = new LeveragedOrders

    val tokenRead = txContext.kernelTransaction().tokenRead()
    val stats = TransactionBoundGraphStatistics(txContext, NullLog.getInstance())

    val labelMap = tokenRead.labelsGetAllTokens().asScala.map(l => l.name() -> l.id()).toMap
    val relMap = tokenRead.relationshipTypesGetAllTokens().asScala.toVector.map(r => r.name() -> r.id()).toMap

    for {
      WithState(logicalPlan, state) <- new LogicalPlanGenerator(labelMap, relMap, stats, costLimit, nodes, rels).logicalPlan
    } yield WithState(LogicalQuery(logicalPlan,
      "<<queryText>>",
      readOnly = true,
      logicalPlan.availableSymbols.toArray,
      state.semanticTable,
      state.cardinalities,
      providedOrders,
      leveragedOrders,
      hasLoadCSV = false,
      None,
      state.idGen), state)
  }
}
