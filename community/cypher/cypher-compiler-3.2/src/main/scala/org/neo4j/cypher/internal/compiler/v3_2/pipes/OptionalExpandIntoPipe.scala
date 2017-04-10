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
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.mutable.ListBuffer

case class OptionalExpandIntoPipe(source: Pipe, fromName: String, relName: String, toName: String,
                                  dir: SemanticDirection, types: LazyTypes, predicate: Predicate)
                                 (val id: Id = new Id)
                                 (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with CachingExpandInto {
  private final val CACHE_SIZE = 100000

  private val _relationships : ThreadLocal[Iterator[Relationship] with AutoCloseable] =
    new ThreadLocal[Iterator[Relationship] with AutoCloseable]


  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //cache of known connected nodes
    val relCache = new RelationshipsCache(CACHE_SIZE)

    input.flatMap {
      row =>
        closeIterator()
        getRowNode(row, fromName) match {
          case fromNode: Node =>
            val toNode = getRowNode(row, toName)

            if (toNode == null) Iterator.single(row.newWith1(relName, null))
            else {
              val relationships = {
                val maybeRelationships = relCache.get(fromNode, toNode, dir)
                if (maybeRelationships.isDefined)
                  maybeRelationships.get.iterator
                else {
                  val iterator = findRelationships(state.query, fromNode, toNode, relCache, dir, types.types(state.query))
                  _relationships.set(iterator)
                  iterator
                }
              }

              val filteredRows = ListBuffer.empty[ExecutionContext]
              while (relationships.hasNext) {
                val candidateRow = row.newWith1(relName, relationships.next())

                if (predicate.isTrue(candidateRow)(state)) {
                  filteredRows.append(candidateRow)
                }
              }

              if (filteredRows.isEmpty) Iterator.single(row.newWith1(relName, null))
              else filteredRows
            }

          case null => Iterator(row.newWith1(relName, null))
        }
    }
  }

  private def closeIterator() = {
    val closeable = _relationships.get()
    if (closeable != null) closeable.close()
  }

  override def close(success: Boolean): Unit = {
    super.close(success)
    closeIterator()
    _relationships.remove()
  }
}
