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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan._
import org.neo4j.cypher.internal.compiler.v3_0.pipes.matching.{Trail, TraversalMatcher}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.KeyNames

import scala.collection.JavaConverters._

case class TraversalMatchPipe(source: Pipe, matcher: TraversalMatcher, trail: Trail)
                             (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {

  trail.predicates.foreach(_.foreach(_.registerOwningPipe(this)))

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
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
    source.planDescription.andThen(this.id, "TraversalMatcher", variables, KeyNames(trail.pathDescription))

  override def localEffects = {
    // Add effects from matching nodes and relationships
    val matcherEffects =
      if (trail.typ.isEmpty) Effects(ReadsRelationshipBoundNodes, ReadsAllRelationships)
      else trail.typ.foldLeft(Effects(ReadsRelationshipBoundNodes)) { (effects, typ) =>
        effects ++ Effects(ReadsRelationshipsWithTypes(typ))
      }
    // Add effects from predicates
    val allEffects = trail.predicates.flatten.foldLeft(matcherEffects)(_ ++ _.effects(symbols))
    if (isLeaf) allEffects.asLeafEffects else allEffects
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }
}
