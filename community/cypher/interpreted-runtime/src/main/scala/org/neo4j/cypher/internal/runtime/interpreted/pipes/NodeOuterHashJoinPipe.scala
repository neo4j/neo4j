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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.kernel.impl.util.collection
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.kernel.impl.util.collection.EagerBuffer.GROW_NEW_CHUNKS_BY_100_PCT
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class NodeOuterHashJoinPipe(nodeVariables: Set[String], lhs: Pipe, nullableVariables: Set[String])
    extends PipeWithSource(lhs) {

  private val myVariables = nodeVariables.toIndexedSeq
  private val nullVariables: Array[(String, AnyValue)] = nullableVariables.map(_ -> Values.NO_VALUE).toArray

  @nowarn("msg=return statement")
  protected def computeKey(context: CypherRow): Option[LongArray] = {
    val key = new Array[Long](myVariables.length)

    for (idx <- myVariables.indices) {
      key(idx) = context.getByName(myVariables(idx)) match {
        case n: VirtualNodeValue => n.id
        case _                   => return None
      }
    }
    Some(Values.longArray(key))
  }

  protected def addNulls(in: CypherRow): CypherRow = {
    val withNulls = rowFactory.copyWith(in)
    withNulls.set(nullVariables)
    withNulls
  }

  protected def buildProbeTableAndFindNullRows(
    input: ClosingIterator[CypherRow],
    memoryTracker: MemoryTracker,
    withNulls: Boolean
  ): ProbeTable = {
    val probeTable = new ProbeTable(memoryTracker)

    for (context <- input) {
      val key = computeKey(context)

      key match {
        case Some(joinKey) => probeTable.addValue(joinKey, context)
        case None          => if (withNulls) probeTable.addNull(context)
      }
    }

    probeTable
  }
}

//noinspection ReferenceMustBePrefixed
class ProbeTable(memoryTracker: MemoryTracker) extends DefaultCloseListenable {

  private[this] var table: collection.ProbeTable[LongArray, CypherRow] =
    collection.ProbeTable.createProbeTable[LongArray, CypherRow](memoryTracker)

  private[this] var rowsWithNullInKey: EagerBuffer[CypherRow] =
    EagerBuffer.createEagerBuffer[CypherRow](memoryTracker, 16, 8192, GROW_NEW_CHUNKS_BY_100_PCT)

  def addValue(key: LongArray, newValue: CypherRow): Unit = {
    newValue.compact()
    table.put(key, newValue)
  }

  def addNull(context: CypherRow): Unit = {
    context.compact()
    rowsWithNullInKey.add(context)
  }

  def apply(key: LongArray): java.util.Iterator[CypherRow] = table.get(key)

  def keySet: java.util.Set[LongArray] = table.keySet

  def nullRows: ClosingIterator[CypherRow] =
    ClosingIterator(rowsWithNullInKey.autoClosingIterator().asScala).closing(rowsWithNullInKey)

  override def isClosed: Boolean = table == null

  override def closeInternal(): Unit = {
    if (table != null) {
      table.close()
      rowsWithNullInKey.close()
      table = null
      rowsWithNullInKey = null
    }
  }
}
