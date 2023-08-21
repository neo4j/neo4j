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
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

/**
 * Retrieves node counts from count store, for nodes with specified (optional) labels.
 * Can also be used to compute node count for cartesian product of multiple pattern nodes.
 * E.g.,
 * MATCH (n:L1), (n2:L2), (n3) RETURN count(*)
 *
 * @param labels list of labels, of different pattern nodes
 */
case class NodeCountFromCountStorePipe(ident: String, labels: List[Option[LazyLabel]])(val id: Id = Id.INVALID_ID)
    extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    var count = 1L
    val it = labels.iterator
    try {
      while (it.hasNext) {
        it.next() match {
          case Some(lazyLabel) =>
            val idOfLabel = lazyLabel.getId(state.query)
            if (idOfLabel == LazyLabel.UNKNOWN) {
              count = 0
            } else {
              count = Math.multiplyExact(count, state.query.nodeCountByCountStore(idOfLabel))
            }
          case _ =>
            count = Math.multiplyExact(count, state.query.nodeCountByCountStore(NameId.WILDCARD))
        }
      }
    } catch {
      case e: ArithmeticException =>
        throw new org.neo4j.exceptions.ArithmeticException(
          s"Integer overflow, cannot count to number larger than ${Long.MaxValue}",
          e
        )
    }

    val baseContext = state.newRowWithArgument(rowFactory)
    ClosingIterator.single(rowFactory.copyWith(baseContext, ident, Values.longValue(count)))
  }
}
