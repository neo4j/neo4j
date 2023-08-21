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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.stringValue

trait ElementIdFromSlot extends Expression with SlottedExpression {
  override def children: collection.Seq[AstNode[_]] = Seq.empty
}

case class NodeElementIdFromSlot(offset: Int) extends ElementIdFromSlot {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    stringValue(state.query.elementIdMapper().nodeElementId(row.getLongAt(offset)))
  }
}

case class RelationshipElementIdFromSlot(offset: Int) extends ElementIdFromSlot {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    stringValue(state.query.elementIdMapper().relationshipElementId(row.getLongAt(offset)))
  }
}
