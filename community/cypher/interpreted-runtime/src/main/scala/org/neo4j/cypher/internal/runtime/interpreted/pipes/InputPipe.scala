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
import org.neo4j.cypher.internal.runtime.InputCursor
import org.neo4j.cypher.internal.util.attribution.Id

case class InputPipe(variables: Array[String])(val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    new ClosingIterator[CypherRow] {
      var cursor: InputCursor = _
      var cursorOnNextRow: Boolean = false

      override def closeMore(): Unit = if (cursor != null) cursor.close()

      override def innerHasNext: Boolean = {
        while (!cursorOnNextRow) {
          if (cursor == null) {
            cursor = state.input.nextInputBatch()
            if (cursor == null) {
              return false
            }
          }
          if (cursor.next()) {
            cursorOnNextRow = true
          } else {
            cursor.close()
            cursor = null
          }
        }
        true
      }

      override def next(): CypherRow = {
        if (hasNext) {
          val ctx = rowFactory.newRow()
          var i = 0
          while (i < variables.length) {
            ctx.set(variables(i), cursor.value(i))
            i += 1
          }
          cursorOnNextRow = false
          ctx
        } else
          ClosingIterator.empty.next()
      }
    }
  }
}
