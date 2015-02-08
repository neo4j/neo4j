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
package org.neo4j.cypher.internal.compiler.v1_9.pipes

import matching.{PatternGraph, MatchingContext}
import org.neo4j.cypher.internal.compiler.v1_9.commands.Predicate
import org.neo4j.cypher.internal.compiler.v1_9.data.SimpleVal
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.symbols.SymbolTable

class MatchPipe(source: Pipe, predicates: Seq[Predicate], patternGraph: PatternGraph) extends PipeWithSource(source) {
  val matchingContext = new MatchingContext(source.symbols, predicates, patternGraph)
  val symbols = matchingContext.symbols

  protected def internalCreateResults(input: Iterator[ExecutionContext],state: QueryState) = {
    input.flatMap {
      ctx => matchingContext.getMatches(ctx, state)
    }
  }

  override def executionPlanDescription =
    source.executionPlanDescription.andThen(this, "PatternMatch", "g" -> patternGraph)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    //TODO do it
  }
}
