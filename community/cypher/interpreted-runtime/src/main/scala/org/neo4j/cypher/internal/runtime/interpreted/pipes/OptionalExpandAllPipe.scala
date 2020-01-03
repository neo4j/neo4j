/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.{InternalException, ParameterWrongTypeException}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

import scala.collection.Iterator

abstract class OptionalExpandAllPipe(source: Pipe,
                                     fromName: String,
                                     relName: String,
                                     toName: String,
                                     dir: SemanticDirection,
                                     types: RelationshipTypes)
  extends PipeWithSource(source) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row =>
        val fromNode = getFromNode(row)
        fromNode match {
          case n: NodeValue =>
            val relationships = state.query.getRelationshipsForIds(n.id(), dir, types.types(state.query))
            val matchIterator = findMatchIterator(row, state, relationships, n)
            if (matchIterator.isEmpty) {
              Iterator(withNulls(row))
            } else {
              matchIterator
            }

          case value if value eq Values.NO_VALUE =>
            Iterator(withNulls(row))

          case value =>
            throw new ParameterWrongTypeException(s"Expected to find a node at '$fromName' but found $value instead")
        }
    }
  }

  def findMatchIterator(row: ExecutionContext,
                        state: QueryState,
                        relationships: Iterator[RelationshipValue],
                        n: NodeValue): Iterator[ExecutionContext]

  private def withNulls(row: ExecutionContext) = {
    row.set(relName, Values.NO_VALUE, toName, Values.NO_VALUE)
    row
  }

  def getFromNode(row: ExecutionContext): AnyValue = row.getByName(fromName)
}

object OptionalExpandAllPipe {
  def apply(source: Pipe,
            fromName: String,
            relName: String,
            toName: String,
            dir: SemanticDirection,
            types: RelationshipTypes,
            maybePredicate: Option[Expression])(id: Id = Id.INVALID_ID): OptionalExpandAllPipe = maybePredicate match {
    case Some(predicate) => FilteringOptionalExpandAllPipe(source, fromName, relName, toName, dir, types, predicate)(id)
    case None => NonFilteringOptionalExpandAllPipe(source, fromName, relName, toName, dir, types)(id)
  }
}

case class NonFilteringOptionalExpandAllPipe(source: Pipe,
                                             fromName: String,
                                             relName: String,
                                             toName: String,
                                             dir: SemanticDirection,
                                             types: RelationshipTypes)
                                            (val id: Id = Id.INVALID_ID)
  extends OptionalExpandAllPipe(source, fromName, relName, toName, dir, types) {

  override def findMatchIterator(row: ExecutionContext,
                                 ignore: QueryState,
                                 relationships: Iterator[RelationshipValue],
                                 n: NodeValue): Iterator[ExecutionContext] = {
    relationships.map { r =>
      val other = r.otherNode(n)
      executionContextFactory.copyWith(row, relName, r, toName, other)
    }
  }
}

case class FilteringOptionalExpandAllPipe(source: Pipe,
                                          fromName: String,
                                          relName: String,
                                          toName: String,
                                          dir: SemanticDirection,
                                          types: RelationshipTypes,
                                          predicate: Expression)
                                         (val id: Id = Id.INVALID_ID)
  extends OptionalExpandAllPipe(source, fromName, relName, toName, dir, types) {

  predicate.registerOwningPipe(this)

  override def findMatchIterator(row: ExecutionContext,
                                 state: QueryState,
                                 relationships: Iterator[RelationshipValue],
                                 n: NodeValue): Iterator[ExecutionContext] = {
    relationships.map { r =>
      val other = r.otherNode(n)
      executionContextFactory.copyWith(row, relName, r, toName, other)
    }.filter(ctx => predicate(ctx, state) eq Values.TRUE)
  }
}
