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
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE

case class ConditionalApplyPipe(
  source: Pipe,
  inner: Pipe,
  items: Seq[String],
  negated: Boolean,
  rhsOnlySymbols: Set[String]
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] =
    input.flatMap {
      outerContext =>
        if (condition(outerContext)) {
          val innerState = state.withInitialContext(outerContext)
          inner.createResults(innerState)
        } else {
          rhsOnlySymbols.foreach(v => outerContext.set(v, Values.NO_VALUE))
          ClosingIterator.single(outerContext)
        }
    }

  private def condition(context: CypherRow): Boolean = {
    val hasNull = items.map(context.getByName).exists(_ eq NO_VALUE)
    if (negated) hasNull else !hasNull
  }
}
