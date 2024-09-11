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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs

import org.neo4j.common.EntityType
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TracedPath.PathEntity
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipExpansion
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State

case class TracedPath(entities: List[PathEntity]) {
  def length: Int = entities.count(_.entityType == EntityType.RELATIONSHIP)
  def target: Long = entities.last.id
  def ids: Seq[Long] = entities.map(_.id)

  override def toString: String = {
    val sb = new StringBuilder("(")
    var last: TracedPath.PathEntity = null
    for (e <- entities) {
      e.entityType match {
        case EntityType.NODE =>
          if (last == null || (last.entityType eq EntityType.RELATIONSHIP)) {
            sb.append(e.id)
            if (e.slotOrName ne SlotOrName.none) {
              sb.append("@").append(e.slotOrName)
            }
          } else if (last.slotOrName ne e.slotOrName) {
            sb.append(",").append(e.slotOrName)
          }

        case EntityType.RELATIONSHIP => sb.append(")-[").append(e.id).append("]->(")
      }
      last = e
    }
    sb.append(")")
    sb.toString
  }
}

object TracedPath {
  case class PathEntity(slotOrName: SlotOrName, id: Long, entityType: EntityType)

  def fromSignpostStack(stack: SignpostStack): TracedPath = {
    val list = List.newBuilder[PathEntity]
    stack.materialize(new PathWriter {
      def writeNode(slotOrName: SlotOrName, id: Long): Unit =
        list += PathEntity(slotOrName, id, EntityType.NODE)

      def writeRel(slotOrName: SlotOrName, id: Long): Unit =
        list += PathEntity(slotOrName, id, EntityType.RELATIONSHIP)
    })
    new TracedPath(list.result())
  }

  object PathEntity {

    def fromNode(state: State, id: Long): PathEntity =
      PathEntity(state.slotOrName, id, EntityType.NODE)

    def fromRel(re: RelationshipExpansion, id: Long): PathEntity =
      PathEntity(re.slotOrName, id, EntityType.RELATIONSHIP)
  }
}
