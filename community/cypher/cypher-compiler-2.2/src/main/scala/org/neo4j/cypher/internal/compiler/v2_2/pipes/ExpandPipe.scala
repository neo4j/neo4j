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

import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.cypher.internal.compiler.v2_2.{ExecutionContext, InternalException}
import org.neo4j.graphdb.{Direction, Node, Relationship}

sealed abstract class ExpandPipe[T](source: Pipe,
                                    from: String,
                                    relName: String,
                                    to: String,
                                    dir: Direction,
                                    types: Seq[T],
                                    pipeMonitor: PipeMonitor)
                    extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  def planDescription =
    source.planDescription.andThen(this, "Expand", identifiers, ExpandExpression(from, relName, to, dir))

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

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
    new ExpandPipeIteratorForIntTypes(input, from, relName, to, dir, types)(state.query).createResults

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

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
    new ExpandPipeIteratorForStringTypes(input, from, relName, to, dir, types)(state.query).createResults

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Long) = copy()(Some(estimated))
}

sealed abstract
class ExpandPipeIterator[T](input: Iterator[ExecutionContext],
                            from: String,
                            relName: String,
                            to: String,
                            dir: Direction)(implicit qtx: QueryContext) {

  final def createResults: Iterator[ExecutionContext] =
    input.flatMap {
      row =>
        getFromNode(row) match {
          case n: Node =>
            val relationships: Iterator[Relationship] = getRelationships(n)
            relationships.map {
              case r =>
                row.newWith2(relName, r, to, r.getOtherNode(n))
            }

          case null =>
            None

          case value =>
            throw new InternalException(s"Expected to find a node at $from but found $value instead")
        }
    }

  private final def getFromNode(row: ExecutionContext): Any =
    row.getOrElse(from, throw new InternalException(s"Expected to find a node at $from but found nothing"))

  protected def getRelationships(node: Node): Iterator[Relationship]
}

final class ExpandPipeIteratorForIntTypes(input: Iterator[ExecutionContext],
                                          from: String,
                                          relName: String,
                                          to: String,
                                          dir: Direction,
                                          types: Seq[Int])(implicit qtx: QueryContext)
  extends ExpandPipeIterator[Int](input, from, relName, to, dir) {

  protected def getRelationships(node: Node): Iterator[Relationship] =
    qtx.getRelationshipsForIds(node, dir, types)
}

final class ExpandPipeIteratorForStringTypes(input: Iterator[ExecutionContext],
                                             from: String,
                                             relName: String,
                                             to: String,
                                             dir: Direction,
                                             types: Seq[String])(implicit qtx: QueryContext)
  extends ExpandPipeIterator[Int](input, from, relName, to, dir) {

  protected def getRelationships(node: Node): Iterator[Relationship] =
    qtx.getRelationshipsFor(node, dir, types)
}
