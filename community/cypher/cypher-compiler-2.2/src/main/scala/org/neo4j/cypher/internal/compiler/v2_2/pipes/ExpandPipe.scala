/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.ast.RelTypeName
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.PlanDescription.Arguments.IntroducedIdentifier
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.{Direction, Node, Relationship}

sealed abstract class ExpandPipe[T](source: Pipe,
                                 from: String,
                                 relName: String,
                                 to: String,
                                 dir: Direction,
                                 types: Seq[T],
                                 pipeMonitor: PipeMonitor)
                    extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  def getRelationships: (Node, QueryContext, Direction) => Iterator[Relationship]

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row =>
        getFromNode(row) match {
          case n: Node =>
            val relationships: Iterator[Relationship] = getRelationships(n, state.query, dir)
            relationships.map {
              case r =>
                row.newWith2(relName, r, to, r.getOtherNode(n))
            }

          case null => None

          case value => throw new InternalException(s"Expected to find a node at $from but found $value instead")
        }
    }
  }

  def getFromNode(row: ExecutionContext): Any =
    row.getOrElse(from, throw new InternalException(s"Expected to find a node at $from but found nothing"))

  def planDescription = {
    val arguments = Seq(IntroducedIdentifier(relName), IntroducedIdentifier(to))
    source.planDescription.andThen(this, "Expand", arguments:_*)
  }

  val symbols = source.symbols.add(to, CTNode).add(relName, CTRelationship)

  override def localEffects = Effects.READS_ENTITIES
}

case class ExpandPipeForIntTypes(source: Pipe,
                                from: String,
                                relName: String,
                                to: String,
                                dir: Direction,
                                types: Seq[Int])
                               (val estimatedCardinality: Option[Long] = None)
                               (implicit pipeMonitor: PipeMonitor)
  extends ExpandPipe[Int](source, from, relName, to, dir, types, pipeMonitor) {

  override def getRelationships: (Node, QueryContext, Direction) => Iterator[Relationship] =
    (n:Node, query: QueryContext, dir:Direction) => query.getRelationshipsForIds(n, dir, types)


  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Long) = copy()(Some(estimated))
}

case class ExpandPipeForStringTypes(source: Pipe,
                                    from: String,
                                    relName: String,
                                    to: String,
                                    dir: Direction,
                                    types: Seq[String])
                                   (val estimatedCardinality: Option[Long] = None)
                                   (implicit pipeMonitor: PipeMonitor)
  extends ExpandPipe[String](source, from, relName, to, dir, types, pipeMonitor) {

  override def getRelationships: (Node, QueryContext, Direction) => Iterator[Relationship] =
    (n:Node, query: QueryContext, dir:Direction) => query.getRelationshipsFor(n, dir, types)


  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Long) = copy()(Some(estimated))
}
