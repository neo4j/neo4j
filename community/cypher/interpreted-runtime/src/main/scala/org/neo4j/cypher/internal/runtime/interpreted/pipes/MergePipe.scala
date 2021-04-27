/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.SideEffect
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LockNodesPipe.getNodes
import org.neo4j.cypher.internal.util.attribution.Id

class MergePipe(src: Pipe,
                createOps: Array[SideEffect],
                matchOps: Array[SideEffect])(val id: Id = Id.INVALID_ID) extends PipeWithSource(src) {
  override protected def internalCreateResults(input: ClosingIterator[CypherRow],
                                               state: QueryState): ClosingIterator[CypherRow] = {
    if (input.hasNext) {
      input.map(r => {
        performOnMatchOps(state, r)
        r
      })
    } else {
      onNoMatch(state)
    }
  }

  protected def onNoMatch(state: QueryState): ClosingIterator[CypherRow] = {
    val row = state.newRowWithArgument(rowFactory)
    performOnCreateOps(state, row)
    ClosingIterator.single(row)
  }

  protected def performOnMatchOps(state: QueryState,
                                  row: CypherRow): Unit = {
    var i = 0
    while (i < matchOps.length) {
      matchOps(i).execute(row, state)
      i += 1
    }
  }

  protected def performOnCreateOps(state: QueryState,
                                  row: CypherRow): Unit = {
    var i = 0
    while (i < createOps.length) {
      createOps(i).execute(row,state)
      i += 1
    }
  }
}

class LockingMergePipe(src: Pipe,
                       createOps: Array[SideEffect],
                       onMatchOps: Array[SideEffect],
                       nodesToLock: Array[String])(id: Id = Id.INVALID_ID) extends MergePipe(src, createOps, onMatchOps)(id) {

  override protected def onNoMatch(state: QueryState): ClosingIterator[CypherRow] = {
    val row = state.newRowWithArgument(rowFactory)
    val longs = getNodes(row, nodesToLock)
    state.query.lockNodes(longs: _*)

    val reRead = src.createResults(state)
    if (reRead.hasNext) {
      reRead.map(r => {
        performOnMatchOps(state, r)
        r
      })
    } else {
      performOnCreateOps(state, row)
      ClosingIterator.single(row)
    }
  }
}
