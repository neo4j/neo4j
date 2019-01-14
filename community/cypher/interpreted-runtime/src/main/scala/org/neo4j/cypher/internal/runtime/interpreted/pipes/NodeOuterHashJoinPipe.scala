/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, MutableList}

abstract class NodeOuterHashJoinPipe(nodeVariables: Set[String], lhs: Pipe, rhs: Pipe, nullableVariables: Set[String]) extends PipeWithSource(lhs) {

  private val myVariables = nodeVariables.toIndexedSeq
  private val nullColumns: Seq[(String, AnyValue)] = nullableVariables.map(_ -> Values.NO_VALUE).toSeq

  protected def computeKey(context: ExecutionContext): Option[IndexedSeq[Long]] = {
    val key = new Array[Long](myVariables.length)

    for (idx <- myVariables.indices) {
      key(idx) = context(myVariables(idx)) match {
        case n: VirtualNodeValue => n.id
        case _ => return None
      }
    }
    Some(key.toIndexedSeq)
  }

  protected def addNulls(in: ExecutionContext): ExecutionContext = executionContextFactory.copyWith(in).set(nullColumns)

  protected def buildProbeTableAndFindNullRows(input: Iterator[ExecutionContext], withNulls: Boolean): ProbeTable = {
    val probeTable = new ProbeTable()

    for (context <- input) {
      val key = computeKey(context)

      key match {
        case Some(joinKey) => probeTable.addValue(joinKey, context)
        case None => if(withNulls) probeTable.addNull(context)
      }
    }

    probeTable
  }
}

//noinspection ReferenceMustBePrefixed
class ProbeTable() {
  private val table: mutable.HashMap[IndexedSeq[Long], MutableList[ExecutionContext]] =
    new mutable.HashMap[IndexedSeq[Long], MutableList[ExecutionContext]]

  private val rowsWithNullInKey: ListBuffer[ExecutionContext] = new ListBuffer[ExecutionContext]()

  def addValue(key: IndexedSeq[Long], newValue: ExecutionContext) {
    val values = table.getOrElseUpdate(key, MutableList.empty)
    values += newValue
  }

  def addNull(context: ExecutionContext): Unit = rowsWithNullInKey += context

  private val EMPTY: MutableList[ExecutionContext] = MutableList.empty

  def apply(key: IndexedSeq[Long]): MutableList[ExecutionContext] = table.getOrElse(key, EMPTY)

  def keySet: collection.Set[IndexedSeq[Long]] = table.keySet

  def nullRows: Iterator[ExecutionContext] = rowsWithNullInKey.iterator
}
