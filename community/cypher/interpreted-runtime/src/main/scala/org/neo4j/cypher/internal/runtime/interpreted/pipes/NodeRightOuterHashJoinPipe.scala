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
import org.neo4j.cypher.internal.runtime.ClosingIterator.ScalaSeqAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.attribution.Id

import scala.jdk.CollectionConverters.IteratorHasAsScala

case class NodeRightOuterHashJoinPipe(nodeVariables: Set[String], lhs: Pipe, rhs: Pipe, nullableVariables: Set[String])(
  val id: Id = Id.INVALID_ID
) extends NodeOuterHashJoinPipe(nodeVariables, lhs, nullableVariables) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {

    val rhsResult = rhs.createResults(state)
    if (rhsResult.isEmpty)
      return ClosingIterator.empty

    val probeTable = buildProbeTableAndFindNullRows(
      input,
      state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x),
      withNulls = false
    )
    state.query.resources.trace(probeTable)
    val result = for {
      rhsRow <- rhsResult
      outputRow <- computeKey(rhsRow) match {
        case Some(joinKey) =>
          val lhsRows = probeTable(joinKey)
          if (lhsRows.hasNext) {
            lhsRows.asScala.map { lhsRow =>
              val outputRow = rowFactory.copyWith(rhsRow)
              // lhs and rhs might have different nullability - should use nullability on rhs
              outputRow.mergeWith(lhsRow, state.query, false)
              outputRow
            }.asClosingIterator
          } else {
            ClosingIterator.single(addNulls(rhsRow))
          }
        case None =>
          ClosingIterator.single(addNulls(rhsRow))
      }
    } yield outputRow

    result.closing(probeTable)
  }
}
