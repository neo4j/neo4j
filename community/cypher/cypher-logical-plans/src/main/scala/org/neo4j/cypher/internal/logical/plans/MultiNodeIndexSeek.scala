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

import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId

/**
 * Produces one or zero rows containing the nodes with the given labels and property values.
 *
 * This operator is used on label/property combinations under uniqueness constraint, meaning that a single matching
 * node is guaranteed per seek.
 */
case class MultiNodeIndexSeek(nodeIndexSeeks: Seq[IndexSeekLeafPlan])
                             (implicit idGen: IdGen) extends MultiNodeIndexLeafPlan(idGen) {

  override val availableSymbols: Set[String] =
    nodeIndexSeeks.flatMap(_.availableSymbols).toSet

  override def usedVariables: Set[String] = nodeIndexSeeks.flatMap(_.usedVariables).toSet

  override def argumentIds: Set[String] =
    nodeIndexSeeks.flatMap(_.argumentIds).toSet

  override def cachedProperties: Seq[CachedProperty] =
    nodeIndexSeeks.flatMap(_.cachedProperties)

  override def properties: Seq[IndexedProperty] =
    nodeIndexSeeks.flatMap(_.properties)

  override def withoutArgumentIds(argsToExclude: Set[String]): MultiNodeIndexLeafPlan =
    copy(nodeIndexSeeks.map(_.withoutArgumentIds(argsToExclude).asInstanceOf[IndexSeekLeafPlan]))(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): MultiNodeIndexLeafPlan =
    MultiNodeIndexSeek(nodeIndexSeeks.map(_.withMappedProperties(f)))(SameId(this.id))

  override def copyWithoutGettingValues: MultiNodeIndexSeek =
    // NOTE: This is only used by a top-down rewriter (removeCachedProperties).
    // Since our generalized tree rewriters will descend into children (including Seq) we do not need to do anything
    this

  override def idNames: Set[String] =
    nodeIndexSeeks.map(_.idName).toSet
}
