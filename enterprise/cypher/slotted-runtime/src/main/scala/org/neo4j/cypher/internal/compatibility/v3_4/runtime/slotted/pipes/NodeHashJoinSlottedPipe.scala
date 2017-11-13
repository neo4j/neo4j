/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.pipes

import java.util

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PipelineInformation
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.helpers.NullChecker.nodeIsNull
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId

import scala.collection.mutable

case class NodeHashJoinSlottedPipe(leftNodes: Array[Int],
                                   rightNodes: Array[Int],
                                   left: Pipe,
                                   right: Pipe,
                                   pipelineInformation: PipelineInformation,
                                   longsToCopy: Array[(Int, Int)],
                                   refsToCopy: Array[(Int, Int)])
                                  (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends PipeWithSource(left) {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    if (input.isEmpty)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(input)

    if (table.isEmpty)
      return Iterator.empty

    val result = for {rhs: ExecutionContext <- rhsIterator
                      joinKey <- computeKey(rhs)}
      yield {
        val matchesFromLhs: mutable.Seq[ExecutionContext] = table.getOrElse(joinKey, mutable.MutableList.empty)

        matchesFromLhs.map{ lhs =>
          val newRow = PrimitiveExecutionContext(pipelineInformation)
          lhs.copyTo(newRow)
          longsToCopy foreach {
            case (from, to) => newRow.setLongAt(to, rhs.getLongAt(from))
          }
          refsToCopy foreach {
            case (from, to) => newRow.setRefAt(to, rhs.getRefAt(from))
          }
          newRow
        }
      }

    result.flatten
  }

  private def buildProbeTable(input: Iterator[ExecutionContext]): mutable.HashMap[HashKey, mutable.MutableList[ExecutionContext]] = {
    val table = new mutable.HashMap[HashKey, mutable.MutableList[ExecutionContext]]

    for {context <- input
         joinKey <- computeKey(context)} {
      val matchingRows = table.getOrElseUpdate(joinKey, mutable.MutableList.empty)
      matchingRows += context
    }

    table
  }

  /**
    * Creates an array of longs to do the hash join on. If any of the nodes is null, nothing will match and we'll simply return a None
    *
    * @param context The execution context to get the node ids from
    * @return A Some[Array] if all nodes are valid, or None if any is null
    */
  private def computeKey(context: ExecutionContext): Option[HashKey] = {
    val key = new Array[Long](leftNodes.length)

    for (idx <- leftNodes) {
      val nodeId = context.getLongAt(idx)
      if (nodeIsNull(nodeId))
        return None
      key(idx) = nodeId
    }
    Some(HashKey(key))
  }

  private case class HashKey(longs: Array[Long]) {
    override def hashCode(): Int = util.Arrays.hashCode(longs)

    override def equals(obj: scala.Any): Boolean = obj match {
      case HashKey(other) => util.Arrays.equals(longs, other)
      case _ => false
    }

    override def canEqual(that: Any): Boolean = that.isInstanceOf[HashKey]
  }

}
