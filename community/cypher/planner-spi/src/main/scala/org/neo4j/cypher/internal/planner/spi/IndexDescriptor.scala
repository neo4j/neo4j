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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.EntityType
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.toValueCategory
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb
import org.neo4j.internal.schema.IndexQuery.IndexQueryType
import org.neo4j.values.storable.ValueCategory

import scala.language.implicitConversions

sealed trait IndexBehaviour
case object SkipAndLimit extends IndexBehaviour
case object EventuallyConsistent extends IndexBehaviour

sealed trait IndexOrderCapability

object IndexOrderCapability {
  case object NONE extends IndexOrderCapability
  case object BOTH extends IndexOrderCapability
}

object IndexDescriptor {

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
      case _: PropertyKeyId =>
        throw new InternalException("Expected LabelId or RelTypeId but go PropertyKeyId")
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

  private def toArrayValueCategory(cypherType: CypherType): ValueCategory =
    cypherType match {
      case _: symbols.AnyType           => ValueCategory.ANYTHING
      case _: symbols.DateType          => ValueCategory.TEMPORAL_ARRAY
      case _: symbols.NodeType          => ValueCategory.UNKNOWN
      case _: symbols.PathType          => ValueCategory.UNKNOWN
      case _: symbols.ZonedTimeType     => ValueCategory.TEMPORAL_ARRAY
      case _: symbols.FloatType         => ValueCategory.NUMBER_ARRAY
      case _: symbols.PointType         => ValueCategory.GEOMETRY_ARRAY
      case _: symbols.NumberType        => ValueCategory.NUMBER_ARRAY
      case _: symbols.StringType        => ValueCategory.TEXT_ARRAY
      case _: symbols.BooleanType       => ValueCategory.BOOLEAN_ARRAY
      case _: symbols.IntegerType       => ValueCategory.NUMBER_ARRAY
      case _: symbols.ZonedDateTimeType => ValueCategory.TEMPORAL_ARRAY
      case _: symbols.DurationType      => ValueCategory.TEMPORAL_ARRAY
      case _: symbols.GeometryType      => ValueCategory.GEOMETRY_ARRAY
      case _: symbols.GraphRefType      => ValueCategory.UNKNOWN
      case _: symbols.LocalTimeType     => ValueCategory.TEMPORAL_ARRAY
      case _: symbols.RelationshipType  => ValueCategory.UNKNOWN
      case _: symbols.LocalDateTimeType => ValueCategory.TEMPORAL_ARRAY
      case _: symbols.MapType           => ValueCategory.UNKNOWN
      case _                            => ValueCategory.UNKNOWN
    }

  def toValueCategory(cypherType: CypherType): ValueCategory = {
    cypherType match {
      case _: symbols.AnyType              => ValueCategory.ANYTHING
      case _: symbols.DateType             => ValueCategory.TEMPORAL
      case _: symbols.NodeType             => ValueCategory.UNKNOWN
      case _: symbols.PathType             => ValueCategory.UNKNOWN
      case _: symbols.ZonedTimeType        => ValueCategory.TEMPORAL
      case _: symbols.FloatType            => ValueCategory.NUMBER
      case _: symbols.PointType            => ValueCategory.GEOMETRY
      case _: symbols.NumberType           => ValueCategory.NUMBER
      case _: symbols.StringType           => ValueCategory.TEXT
      case _: symbols.BooleanType          => ValueCategory.BOOLEAN
      case _: symbols.IntegerType          => ValueCategory.NUMBER
      case _: symbols.ZonedDateTimeType    => ValueCategory.TEMPORAL
      case _: symbols.DurationType         => ValueCategory.TEMPORAL
      case _: symbols.GeometryType         => ValueCategory.GEOMETRY
      case _: symbols.GraphRefType         => ValueCategory.UNKNOWN
      case _: symbols.LocalTimeType        => ValueCategory.TEMPORAL
      case _: symbols.RelationshipType     => ValueCategory.UNKNOWN
      case _: symbols.LocalDateTimeType    => ValueCategory.TEMPORAL
      case _: symbols.MapType              => ValueCategory.UNKNOWN
      case symbols.ListType(cypherType, _) => toArrayValueCategory(cypherType)
      case _                               => ValueCategory.UNKNOWN
    }
  }
}

case class IndexDescriptor(
  indexType: IndexType,
  entityType: EntityType,
  properties: Seq[PropertyKeyId],
  behaviours: Set[IndexBehaviour] = Set.empty[IndexBehaviour],
  orderCapability: IndexOrderCapability = IndexOrderCapability.NONE,
  valueCapability: GetValueFromIndexBehavior = DoNotGetValue,
  maybeKernelIndexCapability: Option[org.neo4j.internal.schema.IndexCapability] = None,
  isUnique: Boolean = false
) {

  // Assert that the index cannot lead to wrong results because of read-committed anomalies.
  (orderCapability, valueCapability) match {
    case (IndexOrderCapability.BOTH, x) if x != CanGetValue =>
      throw new IllegalStateException(
        "Indexes that support ordering must also support returning values. Otherwise queries with 'ORDER BY' might return wrong results under concurrent updates."
      )
    case _ =>
  }

  val isComposite: Boolean = properties.length > 1

  def isQuerySupported(indexQueryType: IndexQueryType, cypherType: CypherType): Boolean = {
    maybeKernelIndexCapability match {
      case Some(indexCapability) =>
        val valueCategory: ValueCategory = toValueCategory(cypherType)
        indexCapability.isQuerySupported(indexQueryType, valueCategory)
      case None => false
    }
  }

  def property: PropertyKeyId =
    if (isComposite) throw new IllegalArgumentException("Cannot get single property of multi-property index")
    else properties.head

  def withBehaviours(bs: Set[IndexBehaviour]): IndexDescriptor = copy(behaviours = bs)
  def withOrderCapability(oc: IndexOrderCapability): IndexDescriptor = copy(orderCapability = oc)
  def withValueCapability(vc: GetValueFromIndexBehavior): IndexDescriptor = copy(valueCapability = vc)

  def withKernelIndexCapability(kqt: org.neo4j.internal.schema.IndexCapability): IndexDescriptor =
    copy(maybeKernelIndexCapability = Some(kqt))
  def unique(setUnique: Boolean = true): IndexDescriptor = copy(isUnique = setUnique)
}

/**
 * (TokenIndex and LookupIndex are the same thing.)
 */
case class TokenIndexDescriptor(
  entityType: org.neo4j.common.EntityType,
  orderCapability: IndexOrderCapability
)
