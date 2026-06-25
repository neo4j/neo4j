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
package org.neo4j.cypher.internal.runtime.cursors

import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.ReferenceCursor
import org.neo4j.lang.CloseListener

class DelegatingReferenceCursor(
  val inner: ReferenceCursor
) extends ReferenceCursor {
  def reference(): Long = inner.reference()
  def next(): Boolean = inner.next()
  def setTracer(tracer: KernelReadTracer): Unit = inner.setTracer(tracer)
  def removeTracer(): Unit = inner.removeTracer()
  def close(): Unit = inner.close()
  def closeInternal(): Unit = inner.closeInternal()
  def isClosed: Boolean = inner.isClosed
  def setCloseListener(closeListener: CloseListener): Unit = inner.setCloseListener(closeListener)
  def setTrackingHandle(token: Int): Unit = inner.setTrackingHandle(token)
  def getTrackingHandle: Int = inner.getTrackingHandle
}
