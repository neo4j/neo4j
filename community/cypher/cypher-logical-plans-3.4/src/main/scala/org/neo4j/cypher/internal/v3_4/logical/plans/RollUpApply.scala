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
  * RollUp is the inverse of the Unwind operator. For each left row,
  * right is executed. For each right row produced, a single column value
  * is extracted and inserted into a collection. which is assigned to 'collectionName'.
  * The left row is produced.
  *
  * It is used for sub queries that return collections, such as pattern expressions (returns
  * a collection of paths) and pattern comprehension.
  *
  * Note about nullableIdentifiers: when any of these identifiers is null, the collection
  * should be null.
  */
case class RollUpApply(left: LogicalPlan,
                       right: LogicalPlan,
                       collectionName: String,
                       variableToCollect: String,
                       nullableVariables: Set[String]
                      )(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {

  override def lhs = Some(left)

  override val availableSymbols: Set[String] = left.availableSymbols + collectionName

  override def rhs = Some(right)
}
