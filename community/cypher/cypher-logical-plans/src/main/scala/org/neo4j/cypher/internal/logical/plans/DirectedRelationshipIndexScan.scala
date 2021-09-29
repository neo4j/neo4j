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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.graphdb.schema.IndexType

/**
 * This operator does a full scan of an index, producing one row per entry.
 *
 *
 *  - `{idName: relationship, startNode: relationship.startNode, endNode: relationship.endNode}`
 */
case class DirectedRelationshipIndexScan(idName: String,
                                         startNode: String,
                                         endNode: String,
                                         override val typeToken: RelationshipTypeToken,
                                         properties: Seq[IndexedProperty],
                                         argumentIds: Set[String],
                                         indexOrder: IndexOrder,
                                         indexType: IndexType)
                                        (implicit idGen: IdGen)
  extends RelationshipIndexLeafPlan(idGen) {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): DirectedRelationshipIndexScan = copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: DirectedRelationshipIndexScan =
    copy(properties = properties.map(_.copy(getValueFromIndex = DoNotGetValue)))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): DirectedRelationshipIndexScan =
    copy(properties = properties.map(f))(SameId(this.id))

  override def leftNode: String = startNode

  override def rightNode: String = endNode
}
