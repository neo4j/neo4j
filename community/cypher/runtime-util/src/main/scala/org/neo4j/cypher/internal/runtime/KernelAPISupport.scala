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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.schema
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueGroup
import org.neo4j.values.storable.Values.NO_VALUE

object KernelAPISupport {

  /**
   * Returns true if the specified property index query would never return any results
   */
  def isImpossibleIndexQuery(query: PropertyIndexQuery): Boolean = query match {
    case null                                    => true
    case p: PropertyIndexQuery.ExactPredicate    => impossibleExactValue(p.value())
    case p: PropertyIndexQuery.RangePredicate[_] => p.valueGroup() == ValueGroup.NO_VALUE
    case _                                       => false
  }

  def impossibleExactValue(value: Value): Boolean = value match {
    case null                   => true
    case NO_VALUE               => true
    case fp: FloatingPointValue => fp.isNaN
    case _                      => false

  }

  def asKernelIndexOrder(indexOrder: plans.IndexOrder): schema.IndexOrder =
    indexOrder match {
      case plans.IndexOrderAscending  => schema.IndexOrder.ASCENDING
      case plans.IndexOrderDescending => schema.IndexOrder.DESCENDING
      case plans.IndexOrderNone       => schema.IndexOrder.NONE
    }
}
