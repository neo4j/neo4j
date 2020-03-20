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
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.attribution.Default

/**
 * Test help utility for hand-writing logical queries.
 */
class LogicalQueryBuilder(tokenResolver: Resolver)
  extends AbstractLogicalPlanBuilder[LogicalQuery, LogicalQueryBuilder](tokenResolver) {

  private var semanticTable = new SemanticTable()

  private val providedOrders: ProvidedOrders = new ProvidedOrders with Default[LogicalPlan, ProvidedOrder] {
    override val defaultValue: ProvidedOrder = ProvidedOrder.empty
  }

  private val cardinalities: Cardinalities = new Cardinalities with Default[LogicalPlan, Cardinality] {
    override val defaultValue: Cardinality = Cardinality.SINGLE
  }

  private val leveragedOrders: LeveragedOrders = new LeveragedOrders

  override def newNode(node: Variable): Unit = {
    semanticTable = semanticTable.addNode(node)
  }

  override def newRelationship(relationship: Variable): Unit = {
    semanticTable = semanticTable.addRelationship(relationship)
  }

  override def newVariable(variable: Variable): Unit = {
    semanticTable = semanticTable.addVariable(variable)
  }

  def withProvidedOrder(order: ProvidedOrder): this.type = {
    providedOrders.set(idOfLastPlan, order)
    this
  }

  def withCardinalityEstimation(cardinality: Cardinality): this.type = {
    cardinalities.set(idOfLastPlan, cardinality)
    this
  }

  def withLeveragedOrder(): this.type = {
    leveragedOrders.set(idOfLastPlan, true)
    this
  }

  def build(readOnly: Boolean = true): LogicalQuery = {
    val logicalPlan = buildLogicalPlan()
    LogicalQuery(logicalPlan,
                 "<<queryText>>",
                 readOnly,
                 resultColumns,
                 semanticTable,
                 cardinalities,
                 providedOrders,
                 leveragedOrders,
                 hasLoadCSV = false,
                 None,
                 idGen)
  }
}
