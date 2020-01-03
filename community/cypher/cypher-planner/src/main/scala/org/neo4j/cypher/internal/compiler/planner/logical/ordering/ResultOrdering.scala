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
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.neo4j.cypher.internal.compiler.helpers.AggregationHelper
import org.neo4j.cypher.internal.ir.{InterestingOrder, ProvidedOrder}
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Property}
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType

/**
  * This object provides some utility methods around InterestingOrder and ProvidedOrder.
  */
object ResultOrdering {

  /**
    * @param interestingOrder the InterestingOrder from the query
    * @param indexProperties  a sequence of the properties (inclusive variable name) of a (composite) index.
    *                         The sequence is length one for non-composite indexes.
    * @param orderTypes       a sequence of the type that the index query compares against for that each property.
    *                         So for `WHERE n.prop = 1 AND n.foo > 'bla'` this will be Seq( CTInt, CTString )
    * @param capabilityLookup a lambda function to ask the index for a (sub)-sequence of types for the order capability it provides.
    *                         With the above example, we would ask the index for its ordering capability for Seq(CTInt, CTString).
    *                         In the future we also want to able to ask it for prefix sequences (e.g. just Seq(CTInt)).
    * @return the order that the index guarantees, if possible in accordance with the given required order.
    */
  def withIndexOrderCapability(interestingOrder: InterestingOrder,
                               indexProperties: Seq[Property],
                               orderTypes: Seq[CypherType],
                               capabilityLookup: Seq[CypherType] => IndexOrderCapability): ProvidedOrder = {

    import InterestingOrder._

    def satisfies(indexProperty: Property, expression: Expression, projections: Map[String, Expression]): Boolean =
      AggregationHelper.extractPropertyForValue(expression, projections).contains(indexProperty)

    if (indexProperties.isEmpty) {
      ProvidedOrder.empty
    } else {
      val indexOrderCapability: IndexOrderCapability = capabilityLookup(orderTypes)
      val candidates = interestingOrder.requiredOrderCandidate +: interestingOrder.interestingOrderCandidates

      val maybeProvidedOrder = candidates.map(_.headOption).collectFirst {
        case Some(Desc(expression, projection)) if indexOrderCapability.desc && satisfies(indexProperties.head, expression, projection) =>
          ProvidedOrder(indexProperties.map { prop => ProvidedOrder.Desc(prop) })

        case Some(Asc(expression, projection)) if indexOrderCapability.asc && satisfies(indexProperties.head, expression, projection) =>
          ProvidedOrder(indexProperties.map { prop => ProvidedOrder.Asc(prop) })
      }

      // If the required order cannot be satisfied, return the index guaranteed order
      maybeProvidedOrder.getOrElse {
        if (indexOrderCapability.asc)
          ProvidedOrder(indexProperties.map { prop => ProvidedOrder.Asc(prop) })
        else if (indexOrderCapability.desc)
          ProvidedOrder(indexProperties.map { prop => ProvidedOrder.Desc(prop) })
        else ProvidedOrder.empty
      }
    }
  }
}
