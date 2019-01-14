/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext

import scala.collection.mutable

abstract class AbstractHashJoinPipe[Key, T](left: Pipe,
                                            right: Pipe,
                                            slots: SlotConfiguration) extends PipeWithSource(left) {
  protected val leftSide: T
  protected val rightSide: T

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    if (input.isEmpty)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(input, state)

    // This will only happen if all the lhs-values evaluate to null, which is probably rare.
    // But, it's cheap to check and will save us from exhausting the rhs, so it's probably worth it
    if (table.isEmpty)
      return Iterator.empty

    val result = for {rhs: ExecutionContext <- rhsIterator
                      joinKey <- computeKey(rhs, rightSide, state)}
      yield {
        val matchesFromLhs: mutable.Seq[ExecutionContext] = table.getOrElse(joinKey, mutable.MutableList.empty)

        matchesFromLhs.map { lhs =>
          val newRow = SlottedExecutionContext(slots)
          lhs.copyTo(newRow)
          copyDataFromRhs(newRow, rhs)
          newRow
        }
      }

    result.flatten
  }

  private def buildProbeTable(input: Iterator[ExecutionContext], queryState: QueryState): mutable.HashMap[Key, mutable.MutableList[ExecutionContext]] = {
    val table = new mutable.HashMap[Key, mutable.MutableList[ExecutionContext]]

    for {context <- input
         joinKey <- computeKey(context, leftSide, queryState)} {
      val matchingRows = table.getOrElseUpdate(joinKey, mutable.MutableList.empty)
      matchingRows += context
    }

    table
  }

  def computeKey(context: ExecutionContext, keyColumns: T, queryState: QueryState): Option[Key]

  def copyDataFromRhs(newRow: SlottedExecutionContext, rhs: ExecutionContext): Unit
}
