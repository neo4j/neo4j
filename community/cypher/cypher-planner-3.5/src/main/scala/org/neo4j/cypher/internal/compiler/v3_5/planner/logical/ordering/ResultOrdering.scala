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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.ordering

import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.IndexOrderCapability
import org.neo4j.cypher.internal.v3_5.util.symbols.CypherType

/**
  * This object provides some utility methods around RequiredOrder and ProvidedOrder.
  */
object ResultOrdering {

  /**
    * @param interestingOrder the InterestingOrder from the query
    * @param properties       a sequence of the properties of a (composite) index. The sequence is length one for non-composite indexes.
    *                         The tuple contains the property name together with the type that the index query compares against for that
    *                         property. So for `WHERE n.prop = 1 AND n.foo > 'bla'` this will be Seq( ('prop',CTInt), ('foo',CTString) )
    * @param capabilityLookup a lambda function to ask the index for a (sub)-sequence of types for the order capability it provides.
    *                         With the above example, we would ask the index for its ordering capability for Seq(CTInt, CTString).
    *                         In the future we also want to able to ask it for prefix sequences (e.g. just Seq(CTInt)).
    * @return the order that the index guarantees, if possible in accordance with the given required order.
    */
  def withIndexOrderCapability(interestingOrder: InterestingOrder,
                               properties: Seq[(String, CypherType)],
                               capabilityLookup: Seq[CypherType] => IndexOrderCapability): ProvidedOrder = {

    import InterestingOrder._

    val orderTypes: Seq[CypherType] = properties.map(_._2)
    val firstInterestingOrder: Option[InterestingOrder.ColumnOrder] = interestingOrder.headOption
    val indexOrderCapability: IndexOrderCapability = capabilityLookup(orderTypes)
    firstInterestingOrder match {
      case Some(_:Desc) if indexOrderCapability.desc =>
        toProvidedOrder(properties.map {case (name, _) => Desc(name)})

      case _ if indexOrderCapability.asc =>
        toProvidedOrder(properties.map {case (name, _) => Asc(name)})

      case _ if indexOrderCapability.desc =>
        toProvidedOrder(properties.map {case (name, _) => Desc(name)})

      case _ => ProvidedOrder.empty
    }
  }

  private def toProvidedOrder(orderColumns: Seq[InterestingOrder.ColumnOrder]): ProvidedOrder =
    ProvidedOrder(orderColumns.map {
      case InterestingOrder.Asc(name) => ProvidedOrder.Asc(name)
      case InterestingOrder.Desc(name) => ProvidedOrder.Desc(name)
    })
}
