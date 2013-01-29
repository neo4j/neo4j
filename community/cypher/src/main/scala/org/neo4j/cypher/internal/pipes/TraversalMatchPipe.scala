/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import collection.JavaConverters._

class TraversalMatchPipe(source: Pipe, matcher: TraversalMatcher, trail: Trail) extends PipeWithSource(source) {

  def createResults(state: QueryState) = {
    source.createResults(state).flatMap {

      case ctx =>
        val paths = matcher.findMatchingPaths(state, ctx)

        paths.flatMap {

          case path =>
            val seq = path.iterator().asScala.toSeq
            trail.decompose(seq).map(ctx.newWith)
        }
    }
  }

  def symbols = trail.symbols(source.symbols)

  def executionPlanDescription() = "TraversalMatcher()"

  def throwIfSymbolsMissing(symbols: SymbolTable) {
  }
}