/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v3_2.pipes.matching.{MatchingContext, PatternGraph}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id

case class MatchPipe(source: Pipe,
                     predicates: Seq[Predicate],
                     patternGraph: PatternGraph,
                     variablesInClause: Set[String])(val id: Id = new Id)
                    (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  val matchingContext = new MatchingContext(source.symbols, predicates, patternGraph, variablesInClause)
  val symbols = matchingContext.symbols
  val variablesBoundInSource = variablesInClause intersect source.symbols.keys.toSet

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    input.flatMap {
      ctx =>
        if (variablesBoundInSource.exists(i => ctx(i) == null))
          None
        else
          matchingContext.getMatches(ctx, state)
    }
  }

  override def planDescription =
    source.planDescription.andThen(this.id, matchingContext.builder.name, variables)

  def mergeStartPoint = matchingContext.builder.startPoint

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(id)
  }
}
