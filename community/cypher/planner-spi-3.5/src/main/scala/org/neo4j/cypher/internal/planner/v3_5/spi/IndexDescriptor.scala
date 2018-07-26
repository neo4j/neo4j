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
package org.neo4j.cypher.internal.planner.v3_5.spi

import org.neo4j.cypher.internal.planner.v3_5.spi.IndexDescriptor.{OrderCapability, ValueCapability}
import org.opencypher.v9_0.util.symbols.CypherType
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

sealed trait IndexLimitation
case object SlowContains extends IndexLimitation

sealed trait IndexOrderCapability
case object BothAscDescIndexOrder extends IndexOrderCapability
case object AscIndexOrder extends IndexOrderCapability
case object DescIndexOrder extends IndexOrderCapability
case object NoIndexOrder extends IndexOrderCapability

object IndexDescriptor {
  /**
    * Given the actual types of properties (one for a single-property index and multiple for a composite index)
    * can this index guarantee ordered retrieval?
    */
  type OrderCapability = Seq[CypherType] => IndexOrderCapability
  val noOrderCapability: OrderCapability = _ => NoIndexOrder

  /**
    * Given the actual types of properties (one for a single-property index and multiple for a composite index)
    * does the index provide the actual values?
    */
  type ValueCapability = Seq[CypherType] => Seq[Boolean]
  val noValueCapability: ValueCapability = s => s.map(_ => false)

  def apply(label: Int, property: Int): IndexDescriptor = IndexDescriptor(LabelId(label), Seq(PropertyKeyId(property)))

  implicit def toKernelEncode(properties: Seq[PropertyKeyId]): Array[Int] = properties.map(_.id).toArray
}

case class IndexDescriptor(label: LabelId,
                           properties: Seq[PropertyKeyId],
                           limitations: Set[IndexLimitation] = Set.empty[IndexLimitation],
                           orderCapability: OrderCapability = IndexDescriptor.noOrderCapability,
                           valueCapability: ValueCapability = IndexDescriptor.noValueCapability) {
  val isComposite: Boolean = properties.length > 1

  def property: PropertyKeyId = if (isComposite) throw new IllegalArgumentException("Cannot get single property of multi-property index") else properties.head
}
