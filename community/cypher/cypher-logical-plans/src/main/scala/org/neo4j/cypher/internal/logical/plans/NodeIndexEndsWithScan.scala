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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId

/**
 * This operator does a full scan of an index, producing rows for all entries that end with a string value
 *
 * It's much slower than an index seek, since all index entries must be examined, but also much faster than an
 * all-nodes scan or label scan followed by a property value filter.
 */
case class NodeIndexEndsWithScan(idName: String,
                                 label: LabelToken,
                                 property: IndexedProperty,
                                 valueExpr: Expression,
                                 argumentIds: Set[String],
                                 indexOrder: IndexOrder)
                                (implicit idGen: IdGen)
  extends IndexLeafPlan(idGen) {

  override def properties: Seq[IndexedProperty] = Seq(property)

  val availableSymbols: Set[String] = argumentIds + idName

  override def copyWithoutGettingValues: NodeIndexEndsWithScan =
    NodeIndexEndsWithScan(idName, label, IndexedProperty(property.propertyKeyToken, DoNotGetValue), valueExpr, argumentIds, indexOrder)(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): IndexLeafPlan =
    NodeIndexEndsWithScan(idName, label, f(property), valueExpr, argumentIds, indexOrder)(SameId(this.id))
}
