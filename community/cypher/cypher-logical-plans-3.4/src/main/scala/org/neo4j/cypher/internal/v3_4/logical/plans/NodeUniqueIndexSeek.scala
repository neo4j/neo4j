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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, LabelToken, PropertyKeyToken}

/**
  * Produces one or zero rows containing the node with the given label and property values.
  *
  * This operator is used on label/property combinations under uniqueness constraint, meaning that a single matching
  * node is guaranteed.
  */
case class NodeUniqueIndexSeek(idName: String,
                               label: LabelToken,
                               propertyKeys: Seq[PropertyKeyToken],
                               valueExpr: QueryExpression[Expression],
                               argumentIds: Set[String])
                              (implicit idGen: IdGen) extends IndexLeafPlan(idGen) {
  override val availableSymbols: Set[String] = argumentIds + idName
}
