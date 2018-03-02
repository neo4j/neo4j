/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

case class NodeRightOuterHashJoinPipe(nodeVariables: Set[String], lhs: Pipe, rhs: Pipe, nullableVariables: Set[String])
                                     (val id: Id = Id.INVALID_ID)
  extends NodeOuterHashJoinPipe(nodeVariables, lhs, rhs, nullableVariables) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    val rhsResult = rhs.createResults(state)
    if (rhsResult.isEmpty)
      return Iterator.empty

    val probeTable = buildProbeTableAndFindNullRows(input, withNulls = false)
    (
      for {rhsRow <- rhsResult}
        yield {
          computeKey(rhsRow) match {
            case Some(joinKey) =>
              val seq = probeTable(joinKey)
              if(seq.nonEmpty) {
                seq.map(lhsRow => executionContextFactory.copyWith(rhsRow).mergeWith(lhsRow))
              } else {
                Seq(addNulls(rhsRow))
              }
            case None =>
              Seq(addNulls(rhsRow))
          }
        }).flatten
  }
}
