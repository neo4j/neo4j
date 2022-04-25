/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.EntityType
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.OrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.ValueCapability
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.graphdb

import scala.language.implicitConversions

sealed trait IndexBehaviour
case object SkipAndLimit extends IndexBehaviour
case object EventuallyConsistent extends IndexBehaviour

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

  def forLabel(indexType: IndexType, labelId: LabelId, properties: Seq[PropertyKeyId]): IndexDescriptor =
    IndexDescriptor(indexType, EntityType.Node(labelId), properties)

  def forRelType(indexType: IndexType, relTypeId: RelTypeId, properties: Seq[PropertyKeyId]): IndexDescriptor =
    IndexDescriptor(indexType, EntityType.Relationship(relTypeId), properties)

  def forNameId(indexType: IndexType, labelOrRelTypeId: NameId, properties: Seq[PropertyKeyId]): IndexDescriptor =
    IndexDescriptor(indexType, EntityType.of(labelOrRelTypeId), properties)

  sealed trait EntityType

  object EntityType {
    final case class Node(label: LabelId) extends EntityType
    final case class Relationship(relType: RelTypeId) extends EntityType

    def of(id: NameId): EntityType = id match {
      case labelId: LabelId     => Node(labelId)
      case relTypeId: RelTypeId => Relationship(relTypeId)
    }
  }

  sealed trait IndexType {
    def toPublicApi: graphdb.schema.IndexType
  }

  object IndexType {

    case object Range extends IndexType {
      override def toPublicApi: graphdb.schema.IndexType = graphdb.schema.IndexType.RANGE
    }

    case object Text extends IndexType {
      override def toPublicApi: graphdb.schema.IndexType = graphdb.schema.IndexType.TEXT
    }

    case object Point extends IndexType {
      override def toPublicApi: graphdb.schema.IndexType = graphdb.schema.IndexType.POINT
    }

    def fromPublicApi(indexType: graphdb.schema.IndexType): Option[IndexType] = indexType match {
      case graphdb.schema.IndexType.RANGE => Some(IndexType.Range)
      case graphdb.schema.IndexType.TEXT  => Some(IndexType.Text)
      case graphdb.schema.IndexType.POINT => Some(IndexType.Point)
      case _                              => None
    }
  }

  implicit def toKernelEncode(properties: Seq[PropertyKeyId]): Array[Int] = properties.map(_.id).toArray
}

case class IndexDescriptor(
  indexType: IndexType,
  entityType: EntityType,
  properties: Seq[PropertyKeyId],
  behaviours: Set[IndexBehaviour] = Set.empty[IndexBehaviour],
  orderCapability: OrderCapability = IndexDescriptor.noOrderCapability,
  valueCapability: ValueCapability = IndexDescriptor.noValueCapability,
  isUnique: Boolean = false
) {
  val isComposite: Boolean = properties.length > 1

  def property: PropertyKeyId =
    if (isComposite) throw new IllegalArgumentException("Cannot get single property of multi-property index")
    else properties.head

  def canEqual(other: Any): Boolean = other.isInstanceOf[IndexDescriptor]

  // The lambda functions `orderCapability` and `valueCapability` cannot sensibly be compared for equality.
  // By excluding them from equals and hashCode, we make the assumption that they should be always the same for (label, properties) combination
  override def equals(other: Any): Boolean = other match {
    case that: IndexDescriptor =>
      (that canEqual this) &&
        indexType == that.indexType &&
        entityType == that.entityType &&
        properties == that.properties &&
        behaviours == that.behaviours &&
        isUnique == that.isUnique
    case _ => false
  }

  override def hashCode(): Int = {
    val state: Seq[Any] = Seq(indexType, entityType, properties, behaviours, isUnique)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def withBehaviours(bs: Set[IndexBehaviour]): IndexDescriptor = copy(behaviours = bs)
  def withOrderCapability(oc: OrderCapability): IndexDescriptor = copy(orderCapability = oc)
  def withValueCapability(vc: ValueCapability): IndexDescriptor = copy(valueCapability = vc)
  def unique(setUnique: Boolean = true): IndexDescriptor = copy(isUnique = setUnique)
}
