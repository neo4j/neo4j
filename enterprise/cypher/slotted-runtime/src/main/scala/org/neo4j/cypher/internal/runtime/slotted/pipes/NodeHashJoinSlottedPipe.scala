/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

case class NodeHashJoinSlottedPipe(leftSide: Array[Int],
                                   rightSide: Array[Int],
                                   left: Pipe,
                                   right: Pipe,
                                   slots: SlotConfiguration,
                                   longsToCopy: Array[(Int, Int)],
                                   refsToCopy: Array[(Int, Int)])
                                  (val id: Id = Id.INVALID_ID)
  extends AbstractHashJoinPipe[HashKey, Array[Int]](left, right, slots) {

  /**
    * Creates an array of longs to do the hash join on. If any of the nodes is null, nothing will match and we'll simply return a None
    *
    * @param context The execution context to get the node ids from
    * @return A Some[Array] if all nodes are valid, or None if any is null
    */
  override def computeKey(context: ExecutionContext, keyColumns: Array[Int], ignored: QueryState): Option[HashKey] = {
    val key = new Array[Long](keyColumns.length)
    for (i <- keyColumns.indices) {
      val idx = keyColumns(i)
      val nodeId = context.getLongAt(idx)
      if (entityIsNull(nodeId))
        return None
      key(i) = nodeId
    }
    Some(HashKey(key))
  }

  override def copyDataFromRhs(newRow: SlottedExecutionContext, rhs: ExecutionContext): Unit = {
    longsToCopy foreach {
      case (from, to) => newRow.setLongAt(to, rhs.getLongAt(from))
    }
    refsToCopy foreach {
      case (from, to) => newRow.setRefAt(to, rhs.getRefAt(from))
    }
  }
}

case class HashKey(longs: Array[Long]) {
  override def hashCode(): Int = util.Arrays.hashCode(longs)

  override def equals(obj: scala.Any): Boolean = obj match {
    case HashKey(other) => util.Arrays.equals(longs, other)
    case _ => false
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[HashKey]
}
