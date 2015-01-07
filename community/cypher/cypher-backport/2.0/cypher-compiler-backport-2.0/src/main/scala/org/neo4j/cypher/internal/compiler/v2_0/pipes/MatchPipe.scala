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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import matching.{PatternGraph, MatchingContext}
import org.neo4j.cypher.internal.compiler.v2_0._
import commands._

case class MatchPipe(source: Pipe,
                     predicates: Seq[Predicate],
                     patternGraph: PatternGraph,
                     identifiersInClause: Set[String]) extends PipeWithSource(source) {
  val matchingContext = new MatchingContext(source.symbols, predicates, patternGraph, identifiersInClause)
  val symbols = matchingContext.symbols
  val identifiersBoundInSource = identifiersInClause intersect source.symbols.keys.toSet

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.flatMap {
      ctx =>
        if (identifiersBoundInSource.exists(i => ctx(i) == null))
          None
        else
          matchingContext.getMatches(ctx, state)
    }
  }

  override def executionPlanDescription =
    source.executionPlanDescription.andThen(this, matchingContext.builder.name, "g" -> patternGraph)
}
