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
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.attribution.{IdGen, SameId}

/**
  * This operator does a full scan of an index, producing rows for all entries that contain a string value
  *
  * It's much slower than an index seek, since all index entries must be examined, but also much faster than an
  * all-nodes scan or label scan followed by a property value filter.
  */
case class NodeIndexContainsScan(idName: String,
                                 label: LabelToken,
                                 property: IndexedProperty,
                                 valueExpr: Expression,
                                 argumentIds: Set[String],
                                 indexOrder: IndexOrder)
                                (implicit idGen: IdGen)
  extends IndexLeafPlan(idGen) {

  override def properties: Seq[IndexedProperty] = Seq(property)

  override def cachedNodeProperties: Traversable[CachedNodeProperty] = property.maybeCachedNodeProperty(idName)

  val availableSymbols: Set[String] = argumentIds + idName

  override def availableCachedNodeProperties: Map[Property, CachedNodeProperty] = property.asAvailablePropertyMap(idName)

  override def copyWithoutGettingValues: NodeIndexContainsScan =
    NodeIndexContainsScan(idName, label, IndexedProperty(property.propertyKeyToken, DoNotGetValue), valueExpr, argumentIds, indexOrder)(SameId(this.id))
}
