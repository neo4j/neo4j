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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.ir.{PlannerQueryPart, ProvidedOrder}
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders, Solveds}
import org.neo4j.cypher.internal.v4_0.util.Cardinality
import org.neo4j.cypher.internal.v4_0.util.attribution.{Attribute, Attributes, IdGen}

object PlanningAttributes {
  class Solveds extends Attribute[LogicalPlan, PlannerQueryPart]
  class Cardinalities extends Attribute[LogicalPlan, Cardinality]
  class ProvidedOrders extends Attribute[LogicalPlan, ProvidedOrder]
}

case class PlanningAttributes(solveds: Solveds, cardinalities: Cardinalities, providedOrders: ProvidedOrders) {
  def asAttributes(idGen: IdGen): Attributes[LogicalPlan] = Attributes[LogicalPlan](idGen, solveds, cardinalities, providedOrders)
}