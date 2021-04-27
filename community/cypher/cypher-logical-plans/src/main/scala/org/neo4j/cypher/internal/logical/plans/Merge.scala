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

import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.util.attribution.IdGen

/**
 * Merge executes the inner plan and on each found row it executes `onMatch`. If there are no found rows
 * it will first run `createOps` followed by `onCreate`
 */
case class Merge(read: LogicalPlan,
                 createNodes: Seq[CreateNode],
                 createRelationships: Seq[CreateRelationship],
                 onMatch: Seq[SetMutatingPattern],
                 onCreate: Seq[SetMutatingPattern],
                 nodesToLock: Set[String])
                (implicit idGen: IdGen) extends LogicalUnaryPlan(idGen) with UpdatingPlan {
  override def source: LogicalPlan = read

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan with UpdatingPlan = copy(read = newLHS)(idGen)

  override def availableSymbols: Set[String] = read.availableSymbols
}
