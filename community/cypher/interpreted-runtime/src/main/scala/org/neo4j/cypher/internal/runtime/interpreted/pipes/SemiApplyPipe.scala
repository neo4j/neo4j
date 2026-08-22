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
import org.neo4j.cypher.internal.util.attribution.Id

case class SemiApplyPipe(source: Pipe, inner: Pipe)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.filter {
      outerContext =>
        val innerState = state.withInitialContext(outerContext)
        val innerResults = inner.createResults(innerState)
        val result = innerResults.hasNext
        if (result) {
          // Pull the first row so that projection expressions in the subquery are actually
          // evaluated. Without this, an EXISTS subquery whose LHS is a projection (WITH/RETURN)
          // would silently skip evaluating the projection expressions (see GH-13938).
          innerResults.next()
        }
        innerResults.close()
        result
    }
  }
}

case class AntiSemiApplyPipe(source: Pipe, inner: Pipe)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.filter {
      outerContext =>
        val innerState = state.withInitialContext(outerContext)
        val innerResults = inner.createResults(innerState)
        val hasNext = innerResults.hasNext
        if (hasNext) {
          innerResults.next()
        }
        val result = !hasNext
        innerResults.close()
        result
    }
  }
}
