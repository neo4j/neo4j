/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.spi.v2_3

import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.core.NodeManager

/**
  * Converts a RelationshipIterator coming from the Kernel API into an Iterator[Relationship] while
  * still sticking to the fact that each relationship record is only loaded once.
  */
class BeansAPIRelationshipIterator(relationships: RelationshipIterator,
                                   nodeManager: NodeManager) extends Iterator[Relationship] {

  private var nextRelationship: Relationship = null
  private val visitor = new RelationshipVisitor[RuntimeException] {
    override def visit(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long) {
      nextRelationship = nodeManager.newRelationshipProxy(relationshipId, startNodeId, typeId, endNodeId)
    }
  }

  override def hasNext: Boolean = relationships.hasNext

  override def next(): Relationship = {
    if (hasNext) {
      val relationshipId = relationships.next()
      relationships.relationshipVisit(relationshipId, visitor)
      nextRelationship
    } else {
      throw new NoSuchElementException
    }
  }
}
