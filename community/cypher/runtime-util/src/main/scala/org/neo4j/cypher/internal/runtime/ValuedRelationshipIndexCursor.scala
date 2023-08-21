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
package org.neo4j.cypher.internal.runtime

import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.storageengine.api.PropertySelection
import org.neo4j.storageengine.api.Reference
import org.neo4j.values.storable.Value

class ValuedRelationshipIndexCursor(val inner: RelationshipValueIndexCursor, values: Array[Value])
    extends DefaultCloseListenable with RelationshipValueIndexCursor {

  override def numberOfProperties(): Int = values.length

  override def hasValue: Boolean = true

  override def propertyValue(offset: Int): Value = values(offset)

  override def next(): Boolean = inner.next()

  override def readFromStore(): Boolean = inner.readFromStore()

  override def closeInternal(): Unit = inner.close()

  override def isClosed: Boolean = inner.isClosed

  override def score(): Float = inner.score()

  override def setTracer(tracer: KernelReadTracer): Unit = inner.setTracer(tracer)

  override def removeTracer(): Unit = inner.removeTracer()

  override def source(cursor: NodeCursor): Unit = inner.source(cursor)

  override def target(cursor: NodeCursor): Unit = inner.target(cursor)

  override def `type`(): Int = inner.`type`()

  override def sourceNodeReference(): Long = inner.sourceNodeReference()

  override def targetNodeReference(): Long = inner.targetNodeReference()

  override def relationshipReference(): Long = inner.relationshipReference()

  override def properties(cursor: PropertyCursor, selection: PropertySelection): Unit =
    inner.properties(cursor, selection)

  override def propertiesReference(): Reference = inner.propertiesReference()
}
