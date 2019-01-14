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

/**
  * For each relationship id in 'relIds', fetch the corresponding relationship. For each relationship,
  * produce one row containing:
  *   - argument
  *   - the relationship as 'idName'
  *   - the start node as 'startNode'
  *   - the end node as 'endNode'
  */
case class DirectedRelationshipByIdSeek(idName: String,
                                        relIds: SeekableArgs,
                                        startNode: String,
                                        endNode: String,
                                        argumentIds: Set[String])(implicit idGen: IdGen)
  extends LogicalLeafPlan(idGen) {

  val availableSymbols: Set[String] = argumentIds ++ Set(idName, startNode, endNode)
}
