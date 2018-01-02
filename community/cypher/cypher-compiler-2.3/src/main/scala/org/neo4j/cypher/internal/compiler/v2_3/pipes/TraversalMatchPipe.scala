/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ReadsAllNodes, Effects, ReadsRelationships}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.{Trail, TraversalMatcher}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames

import scala.collection.JavaConverters._

case class TraversalMatchPipe(source: Pipe, matcher: TraversalMatcher, trail: Trail)
                             (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    input.flatMap {

      case ctx =>
        val paths = matcher.findMatchingPaths(state, ctx)

        paths.flatMap {

          case path =>
            val seq=path.iterator().asScala.toStream // todo map different path implementations better to a list, aka path.toList
            trail.decompose(seq).map(ctx.newWith)
        }
    }
  }

  def symbols = trail.symbols(source.symbols)

  def planDescription =
    source.planDescription.andThen(this.id, "TraversalMatcher", identifiers, KeyNames(trail.pathDescription))

  override def localEffects = trail.predicates.flatten.foldLeft(Effects(ReadsAllNodes, ReadsRelationships))(_ | _.effects(symbols))

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }
}
