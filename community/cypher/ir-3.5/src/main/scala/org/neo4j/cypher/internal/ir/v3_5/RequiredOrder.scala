/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.ir.v3_5

sealed trait RequiredColumnOrder
case object AscColumnOrder extends RequiredColumnOrder
case object DescColumnOrder extends RequiredColumnOrder

object RequiredOrder {
  val empty = RequiredOrder(Seq.empty)
}

/**
  * A single PlannerQuery can require an ordering on its results. This ordering can emerge
  * from an ORDER BY, or even from distinct or aggregation. The requirement on the ordering can
  * be strict (it needs to be ordered for correct results) or weak (ordering will allow for optimizations).
  * A weak requirement might in addition not care about column order and sort direction.
  *
  * Right now this type only encodes strict requirements that emerged because of an ORDER BY.
  *
  * @param columns a sequence of columns names and their required sort direction.
  */
case class RequiredOrder(columns: Seq[(String, RequiredColumnOrder)]) {

  def isEmpty: Boolean = this == RequiredOrder.empty
}
