/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_2.{InternalException, SemanticDirection}
import org.neo4j.graphdb.{Node, Relationship}

case class ExpandAllPipe(source: Pipe,
                         fromName: String,
                         relName: String,
                         toName: String,
                         dir: SemanticDirection,
                         types: LazyTypes)
                        (val id: Id = new Id)
                        (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) {

  private val relationships: ThreadLocal[Iterator[Relationship] with AutoCloseable] =
    new ThreadLocal[Iterator[Relationship] with AutoCloseable]

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row =>
        closeIterator()
        getFromNode(row) match {
          case n: Node =>
            val iterator = state.query.getRelationshipsForIds(n, dir, types.types(state.query))
            relationships.set(iterator)
            iterator.map { r => row.newWith2(relName, r, toName, r.getOtherNode(n)) }

          case null => None

          case value => throw new InternalException(s"Expected to find a node at $fromName but found $value instead")
        }
    }
  }

  def typeNames = types.names

  def getFromNode(row: ExecutionContext): Any =
    row.getOrElse(fromName, throw new InternalException(s"Expected to find a node at $fromName but found nothing"))

  override def close(success: Boolean) = {
    super.close(success)
    closeIterator()
    relationships.remove()
  }

  private def closeIterator() = {
    val closeable = relationships.get()
    if (closeable != null) {
      closeable.close()
    }
  }
}
