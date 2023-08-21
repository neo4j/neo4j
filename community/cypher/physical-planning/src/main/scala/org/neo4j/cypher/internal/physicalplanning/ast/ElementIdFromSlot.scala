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
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

/**
 * Retrieve element id from long slot, requires slot to be non null.
 */
trait ElementIdFromSlot extends RuntimeExpression {
  override def isConstantForQuery: Boolean = false
}

object ElementIdFromSlot {

  def unapply(slot: Slot): Option[RuntimeExpression] = slot match {
    case LongSlot(offset, false, CTNode)         => Some(NodeElementIdFromSlot(offset))
    case LongSlot(offset, true, CTNode)          => Some(NullCheck(offset, NodeElementIdFromSlot(offset)))
    case LongSlot(offset, false, CTRelationship) => Some(RelationshipElementIdFromSlot(offset))
    case LongSlot(offset, true, CTRelationship)  => Some(NullCheck(offset, RelationshipElementIdFromSlot(offset)))
    case _                                       => None
  }
}

case class NodeElementIdFromSlot(offset: Int) extends ElementIdFromSlot

case class RelationshipElementIdFromSlot(offset: Int) extends ElementIdFromSlot
