/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes.matching

import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.cypher.internal.compiler.v1_9.commands.Predicate
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState


class FilteringIterable(inner: Iterable[Relationship],
                        startNode:Node,
                        predicate: Predicate,
                        ctx:ExecutionContext,
                        state:QueryState)
  extends Iterable[Relationship] {

  class FilteringIterator(inner: Iterator[Relationship]) extends Iterator[Relationship] {
    private var nextInLine: Option[Relationship] = null
    private val m:MiniMap = new MiniMap(null, startNode)

    spoolToNextInLine()

    def hasNext: Boolean = nextInLine.nonEmpty

    def next(): Relationship = nextInLine match {
      case None =>
        inner.next()

      case Some(x) =>
        nextInLine = null
        spoolToNextInLine()
        x
    }

    private def spoolToNextInLine() {
      while (inner.hasNext && nextInLine == null) {
        val x = inner.next()

        m.relationship = x
        m.node = x.getOtherNode(startNode)

        if (predicate.isMatch(m)(state)) {
          nextInLine = Some(x)
        }
      }

      if (nextInLine == null) {
        nextInLine = None
      }
    }
  }

  private def filter(r: Relationship, n: Node, ctx: ExecutionContext): Boolean = {
    val m = new MiniMap(r, n)
    predicate.isMatch(m)(state)
  }


  def iterator = new FilteringIterator(inner.iterator)
}
