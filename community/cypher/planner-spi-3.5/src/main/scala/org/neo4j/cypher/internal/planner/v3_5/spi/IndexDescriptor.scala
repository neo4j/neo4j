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
package org.neo4j.cypher.internal.planner.v3_5.spi

import org.neo4j.cypher.internal.planner.v3_5.spi.IndexDescriptor.{OrderCapability, ValueCapability}
import org.neo4j.cypher.internal.v3_5.logical.plans.{DoNotGetValue, GetValueFromIndexBehavior}
import org.neo4j.cypher.internal.v3_5.util.symbols.CypherType
import org.neo4j.cypher.internal.v3_5.util.{LabelId, PropertyKeyId}

sealed trait IndexLimitation
case object SlowContains extends IndexLimitation

sealed trait IndexOrderCapability {
  def asc: Boolean
  def desc: Boolean
}

object IndexOrderCapability {
  protected class BASE(override val asc: Boolean, override val desc: Boolean) extends IndexOrderCapability
  case object NONE extends BASE(false, false)
  case object ASC extends BASE(true, false)
  case object DESC extends BASE(false, true)
  case object BOTH extends BASE(true, true)
}

object IndexDescriptor {
  /**
    * Given the actual types of properties (one for a single-property index and multiple for a composite index)
    * can this index guarantee ordered retrieval?
    */
  type OrderCapability = Seq[CypherType] => IndexOrderCapability
  val noOrderCapability: OrderCapability = _ => IndexOrderCapability.NONE

  /**
    * Given the actual types of properties (one for a single-property index and multiple for a composite index)
    * does the index provide the actual values?
    */
  type ValueCapability = Seq[CypherType] => Seq[GetValueFromIndexBehavior]
  val noValueCapability: ValueCapability = s => s.map(_ => DoNotGetValue)

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

  def canEqual(other: Any): Boolean = other.isInstanceOf[IndexDescriptor]

  // The lambda functions `orderCapability` and `valueCapability`cannot be compared for
  // equality in a sensible way. By excluding them from equals and hashCode, we make
  // the assumption that they should be always the same for (label, properties) combination
  override def equals(other: Any): Boolean = other match {
    case that: IndexDescriptor =>
      (that canEqual this) &&
        label == that.label &&
        properties == that.properties &&
        limitations == that.limitations
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(label, properties, limitations)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
