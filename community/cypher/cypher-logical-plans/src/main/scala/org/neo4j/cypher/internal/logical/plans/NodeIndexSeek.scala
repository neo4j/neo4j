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

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId

/**
 * For every node with the given label and property values, produces rows with that node.
 */
case class NodeIndexSeek(idName: String,
                         override val label: LabelToken,
                         properties: Seq[IndexedProperty],
                         valueExpr: QueryExpression[Expression],
                         argumentIds: Set[String],
                         indexOrder: IndexOrder)
                        (implicit idGen: IdGen) extends NodeIndexSeekLeafPlan(idGen) {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = valueExpr.expressions.flatMap(_.dependencies).map(_.name).toSet

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeIndexSeek = copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: NodeIndexSeek =
    NodeIndexSeek(idName, label, properties.map{ p => IndexedProperty(p.propertyKeyToken, DoNotGetValue, p.entityType) }, valueExpr, argumentIds, indexOrder)(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): NodeIndexSeek =
    NodeIndexSeek(idName, label, properties.map(f), valueExpr, argumentIds, indexOrder)(SameId(this.id))
}

object NodeIndexSeek extends IndexSeekNames {
  override val PLAN_DESCRIPTION_INDEX_SCAN_NAME: String = "NodeIndexScan"
  override val PLAN_DESCRIPTION_INDEX_SEEK_NAME = "NodeIndexSeek"
  override val PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME = "NodeIndexSeekByRange"
  val PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_NAME = "NodeUniqueIndexSeek"
  val PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_RANGE_NAME = "NodeUniqueIndexSeekByRange"
  val PLAN_DESCRIPTION_UNIQUE_LOCKING_INDEX_SEEK_NAME = "NodeUniqueIndexSeek(Locking)"
}

trait IndexSeekNames {
  def PLAN_DESCRIPTION_INDEX_SCAN_NAME: String
  def PLAN_DESCRIPTION_INDEX_SEEK_NAME: String
  def PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME: String
}