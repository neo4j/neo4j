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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.io.IOUtils
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.impl.newapi.TraceableCursor
import org.neo4j.memory.MemoryTracker

/**
 * Cursors which are used during expression evaluation. These are expected to be used within one
 * method call, as opposed to being returned inside an iterator or stream.
 *
 * @param cursorFactory cursor factor to allocate cursors with.
 */
class ExpressionCursors(private[this] var cursorFactory: CursorFactory,
                        private[this] var cursorContext: CursorContext,
                        memoryTracker: MemoryTracker)
  extends DefaultCloseListenable with ResourceManagedCursorPool {

  private[this] var _nodeCursor: NodeCursor = cursorFactory.allocateNodeCursor(cursorContext)
  private[this] var _relationshipScanCursor: RelationshipScanCursor = cursorFactory.allocateRelationshipScanCursor(cursorContext)
  private[this] var _propertyCursor: PropertyCursor = cursorFactory.allocatePropertyCursor(cursorContext, memoryTracker)

  def nodeCursor: NodeCursor = {
    if (_nodeCursor == null) {
      _nodeCursor = cursorFactory.allocateNodeCursor(cursorContext)
    }
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(!_nodeCursor.asInstanceOf[TraceableCursor[_]].returnedToPool)
    _nodeCursor
  }

  def relationshipScanCursor: RelationshipScanCursor = {
    if (_relationshipScanCursor == null) {
      _relationshipScanCursor = cursorFactory.allocateRelationshipScanCursor(cursorContext)
    }
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(!_relationshipScanCursor.asInstanceOf[TraceableCursor[_]].returnedToPool)
    _relationshipScanCursor
  }

  def propertyCursor: PropertyCursor = {
    if (_propertyCursor == null) {
      _propertyCursor = cursorFactory.allocatePropertyCursor(cursorContext, memoryTracker)
    }
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(!_propertyCursor.asInstanceOf[TraceableCursor[_]].returnedToPool)
    _propertyCursor
  }

  override def isClosed: Boolean = {
    _nodeCursor == null && _relationshipScanCursor == null && _propertyCursor == null
  }

  override def closeInternal(): Unit = {
    if (!isClosed) {
      val nodeCursorToClose = _nodeCursor
      val relationshipScanCursorToClose = _relationshipScanCursor
      val propertyCursorToClose = _propertyCursor
      _nodeCursor = null
      _relationshipScanCursor = null
      _propertyCursor = null
      IOUtils.closeAll(nodeCursorToClose, relationshipScanCursorToClose, propertyCursorToClose)
    }
  }

  def setKernelTracer(tracer: KernelReadTracer): Unit = {
    nodeCursor.setTracer(tracer)
    relationshipScanCursor.setTracer(tracer)
    propertyCursor.setTracer(tracer)
  }

  override def closeCursors(): Unit = closeInternal()

  override def setCursorFactoryAndContext(cursorFactory: CursorFactory, cursorContext: CursorContext): Unit = {
    this.cursorFactory = cursorFactory
    this.cursorContext = cursorContext
  }
}
