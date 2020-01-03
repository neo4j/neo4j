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
package org.neo4j.cypher.internal.runtime

import org.neo4j.internal.kernel.api._
import org.neo4j.io.IOUtils

/**
  * Cursors which are used during expression evaluation. These are expected to be used within one
  * method call, as opposed to being returned inside an iterator or stream.
  *
  * @param cursorFactory cursor factor to allocate cursors with.
  */
class ExpressionCursors(cursorFactory: CursorFactory) extends DefaultCloseListenable with AutoCloseablePlus {
  val nodeCursor: NodeCursor = cursorFactory.allocateNodeCursor()
  val relationshipScanCursor: RelationshipScanCursor = cursorFactory.allocateRelationshipScanCursor()
  val propertyCursor: PropertyCursor = cursorFactory.allocatePropertyCursor()

  override def isClosed: Boolean = {
    nodeCursor.isClosed && relationshipScanCursor.isClosed && propertyCursor.isClosed
  }

  override def close(): Unit = {
    closeInternal()
    val listener = closeListener
    if (listener != null) listener.onClosed(this)
  }

  override def closeInternal(): Unit = {
    if (!isClosed) {
      IOUtils.closeAll(nodeCursor, relationshipScanCursor, propertyCursor)
    }
  }
}
