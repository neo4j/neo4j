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
package org.neo4j.internal.kernel.api.helpers.traversal

import org.neo4j.internal.kernel.api.RelationshipTraversalEntities

import java.util.function.Predicate

class ReversedRelTraversalEntities(inner: RelationshipTraversalEntities) extends RelationshipTraversalEntities {
  def relationshipReference(): Long = inner.relationshipReference()
  def `type`(): Int = inner.`type`()
  def sourceNodeReference(): Long = inner.sourceNodeReference()
  def targetNodeReference(): Long = inner.targetNodeReference()

  // these two references are swapped
  def otherNodeReference(): Long = inner.originNodeReference()
  def originNodeReference(): Long = inner.otherNodeReference()
}

class ReversedRelTraversalEntitiesPredicate(inner: Predicate[RelationshipTraversalEntities])
    extends Predicate[RelationshipTraversalEntities] {

  def test(cursor: RelationshipTraversalEntities): Boolean =
    inner.test(new ReversedRelTraversalEntities(cursor))
}
