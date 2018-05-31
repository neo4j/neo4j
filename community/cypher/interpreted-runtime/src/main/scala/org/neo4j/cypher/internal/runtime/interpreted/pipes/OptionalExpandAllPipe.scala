/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.opencypher.v9_0.util.InternalException
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.expressions.SemanticDirection
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

case class OptionalExpandAllPipe(source: Pipe, fromName: String, relName: String, toName: String, dir: SemanticDirection,
                                 types: LazyTypes, predicate: Predicate)
                                (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  predicate.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row =>
        val fromNode = getFromNode(row)
        fromNode match {
          case n: NodeValue =>
            val relationships = state.query.getRelationshipsForIds(n.id(), dir, types.types(state.query))
            val matchIterator = relationships.map { r =>
                val other = r.otherNode(n)
                executionContextFactory.copyWith(row, relName, r, toName, other)
            }.filter(ctx => predicate.isTrue(ctx, state))

            if (matchIterator.isEmpty) {
              Iterator(withNulls(row))
            } else {
              matchIterator
            }

          case value if value == Values.NO_VALUE =>
            Iterator(withNulls(row))

          case value =>
            throw new InternalException(s"Expected to find a node at $fromName but found $value instead")
        }
    }
  }

  private def withNulls(row: ExecutionContext) =
    row.set(relName, Values.NO_VALUE, toName, Values.NO_VALUE)

  def getFromNode(row: ExecutionContext): AnyValue =
    row.getOrElse(fromName, throw new InternalException(s"Expected to find a node at $fromName but found nothing"))
}
