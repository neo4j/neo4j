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

import org.neo4j.internal.kernel.api.AutoCloseablePlus
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.io.IOUtils
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.memory.MemoryTracker

/**
 * Cursors which are used during expression evaluation. These are expected to be used within one
 * method call, as opposed to being returned inside an iterator or stream.
 *
 * @param cursorFactory cursor factor to allocate cursors with.
 */
class ExpressionCursors(cursorFactory: CursorFactory, cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker) extends DefaultCloseListenable with AutoCloseablePlus {
  val nodeCursor: NodeCursor = cursorFactory.allocateNodeCursor(cursorTracer)
  val relationshipScanCursor: RelationshipScanCursor = cursorFactory.allocateRelationshipScanCursor(cursorTracer)
  val propertyCursor: PropertyCursor = cursorFactory.allocatePropertyCursor(cursorTracer, memoryTracker)

  override def isClosed: Boolean = {
    nodeCursor.isClosed && relationshipScanCursor.isClosed && propertyCursor.isClosed
  }

  override def closeInternal(): Unit = {
    if (!isClosed) {
      IOUtils.closeAll(nodeCursor, relationshipScanCursor, propertyCursor)
    }
  }
}
