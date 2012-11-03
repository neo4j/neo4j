/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

import matching.{Trail, TraversalMatcher}
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.graphdb.Path
import collection.JavaConverters._

class TraversalMatchPipe(source: Pipe, matcher: TraversalMatcher, trail: Trail) extends PipeWithSource(source) {

  def createResults(state: QueryState) = {
    val sourceResults = source.createResults(state)
    new MatchProducer(sourceResults, state)
  }

  def symbols = trail.symbols(source.symbols)

  def executionPlan() = "TraversalMatcher()"

  def assertTypes(symbols: SymbolTable) {}

  class MatchProducer(input: Iterator[ExecutionContext], state: QueryState) extends Iterator[ExecutionContext] {
    var pathBuffer: Iterator[Path] = Iterator()
    var mapBuffer: Iterator[Map[String, Any]] = Iterator()
    var currentContext: ExecutionContext = null

    var current: Option[ExecutionContext] = null

    def hasNext = current match {
      case null => spoolToNext(); current.nonEmpty
      case _    => current.nonEmpty
    }

    def next(): ExecutionContext = current match {
      case null =>
        spoolToNext()
        next()

      case None =>
        throw new NoSuchElementException("next on empty iterator")

      case Some(result) =>
        spoolToNext()
        result
    }

    private def spoolToNext() {
      def getNextFromMapBuffer =
        Some(currentContext.newWith(mapBuffer.next()))

      def getNextFromPathBuffer = {
        val p: Path = pathBuffer.next()
        val seq = p.iterator().asScala.toSeq
        trail.decompose(seq).toIterator
      }

      def getNextFromInput = {
        currentContext = input.next()
        matcher.findMatchingPaths(state, currentContext)
      }

      if (mapBuffer.hasNext) {
        current = getNextFromMapBuffer
      } else
        if (pathBuffer.hasNext) {
          mapBuffer = getNextFromPathBuffer
          spoolToNext()
        } else
          if (input.hasNext) {
            pathBuffer = getNextFromInput
            spoolToNext()
          } else {
            current = None
          }
    }

    override def toString() = current match {
      case null => "uninitialized iterator"
      case _    => super.toString()
    }
  }

}