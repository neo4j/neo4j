/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.logical.builder.LogicalPlanGenerator
import org.neo4j.cypher.internal.logical.builder.LogicalPlanGenerator.WithState
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders}
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.attribution.Default
import org.scalacheck.Gen

class LogicalQueryGenerator(labels: Seq[String], relTypes: Seq[String]) {

  def logicalQuery: Gen[WithState[LogicalQuery]] = {
    val providedOrders: ProvidedOrders = new ProvidedOrders with Default[LogicalPlan, ProvidedOrder] {
      override val defaultValue: ProvidedOrder = ProvidedOrder.empty
    }
    val cardinalities = new Cardinalities with Default[LogicalPlan, Cardinality] {
      override protected def defaultValue: Cardinality = Cardinality(1)
    }
    for {
      WithState(logicalPlan, state) <- new LogicalPlanGenerator(labels, relTypes).logicalPlan
    } yield WithState(LogicalQuery(logicalPlan,
      "<<queryText>>",
      readOnly = true,
      logicalPlan.availableSymbols.toArray,
      state.semanticTable,
      cardinalities,
      providedOrders,
      hasLoadCSV = false,
      None), state)
  }
}
