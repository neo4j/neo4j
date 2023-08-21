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
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.kernel.impl.util.collection
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

import scala.annotation.nowarn

case class NodeHashJoinPipe(nodeVariables: Set[String], left: Pipe, right: Pipe)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(left) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    if (input.isEmpty)
      return ClosingIterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return ClosingIterator.empty

    val table = buildProbeTable(input, state)
    state.query.resources.trace(table)

    if (table.isEmpty) {
      table.close()
      return ClosingIterator.empty
    }

    val result =
      for {
        rhsRow <- rhsIterator
        joinKey <- computeKey(rhsRow)
        lhsRow <- table.get(joinKey).asClosingIterator
      } yield {
        val output = lhsRow.createClone()
        output.mergeWith(rhsRow, state.query)
        output
      }

    result.closing(table)
  }

  private def buildProbeTable(
    input: ClosingIterator[CypherRow],
    queryState: QueryState
  ): collection.ProbeTable[LongArray, CypherRow] = {
    val table = collection.ProbeTable.createProbeTable[LongArray, CypherRow](
      queryState.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    )

    for {
      context <- input
      joinKey <- computeKey(context)
    } {
      table.put(joinKey, context)
    }

    table
  }

  private val cachedVariables = nodeVariables.toIndexedSeq

  @nowarn("msg=return statement")
  private def computeKey(context: CypherRow): ClosingIterator[LongArray] = {
    val key = new Array[Long](cachedVariables.length)

    for (idx <- cachedVariables.indices) {
      key(idx) = context.getByName(cachedVariables(idx)) match {
        case n: VirtualNodeValue => n.id()
        case IsNoValue()         => return ClosingIterator.empty
        case _ => throw new CypherTypeException("Created a plan that uses non-nodes when expecting a node")
      }
    }
    ClosingIterator.single(Values.longArray(key))
  }
}
