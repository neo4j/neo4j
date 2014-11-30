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
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{ExpandInto, ExpandAll, ExpansionMode}
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.cypher.internal.compiler.v2_2.{ExecutionContext, InternalException}
import org.neo4j.cypher.internal.helpers.Generator
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
                                types: Seq[Int],
                                mode: ExpansionMode = ExpandAll)
                               (val estimatedCardinality: Option[Long] = None)
                               (implicit pipeMonitor: PipeMonitor)
  extends ExpandPipe[Int](source, from, relName, to, dir, types, pipeMonitor) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = mode match {
    case ExpandAll =>
      new ExpandPipeIteratorForIntTypes(input, from, relName, to, dir, types)(state.query) with ExpandAllPipeIterator
    case ExpandInto =>
      new ExpandPipeIteratorForIntTypes(input, from, relName, to, dir, types)(state.query) with ExpandIntoPipeIterator
  }

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
                                    types: Seq[String],
                                    mode: ExpansionMode = ExpandAll)
                                   (val estimatedCardinality: Option[Long] = None)
                                   (implicit pipeMonitor: PipeMonitor)
  extends ExpandPipe[String](source, from, relName, to, dir, types, pipeMonitor) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = mode match {
    case ExpandAll =>
      new ExpandPipeIteratorForStringTypes(input, from, relName, to, dir, types)(state.query) with ExpandAllPipeIterator
    case ExpandInto =>
      new ExpandPipeIteratorForStringTypes(input, from, relName, to, dir, types)(state.query) with ExpandIntoPipeIterator
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Long) = copy()(Some(estimated))
}

sealed abstract
class ExpandPipeIterator(input: Iterator[ExecutionContext],
                         protected val from: String,
                         protected val relName: String,
                         protected val to: String,
                         protected val dir: Direction)(implicit qtx: QueryContext)
  extends Generator[ExecutionContext] {

  protected var row: ExecutionContext = null
  protected var node: Node = null
  protected var relationships: Iterator[Relationship] = Iterator.empty

  protected final def prepareNext(): Unit =
    do {
      if (relationships.hasNext) {
        if (prepareNextRelationship)
          return
      } else {
        prepareNextRow()
      }
    } while (isOpen)


  private final def prepareNextRow(): Unit = {
    while (input.hasNext) {
      row = input.next()
      row.getOrElse(from, throw new InternalException(s"Expected to find a node at $from but found nothing")) match {
        case nextNode: Node =>
          node = nextNode
          relationships = expandNode
          return

        case null =>
          // just loop

        case value =>
          throw new InternalException(s"Expected to find a node at $from but found $value instead")
      }
    }
    close()
  }

  protected def prepareNextRelationship: Boolean
  protected def expandNode: Iterator[Relationship]
}

trait ExpandAllPipeIterator {
  self: ExpandPipeIterator =>

  override protected def prepareNextRelationship = true

  override protected def deliverNext = {
    val rel = relationships.next()
    row.newWith2(relName, rel, to, rel.getOtherNode(node))
  }
}

trait ExpandIntoPipeIterator {
  self: ExpandPipeIterator =>

  private var rel: Relationship = null
  private var other: Node = null

  override protected def prepareNextRelationship: Boolean = {
    do {
      rel = relationships.next()
      other = rel.getOtherNode(node)
      val sibling = row(to)
      if (sibling == other) {
        return true
      }
    } while (relationships.hasNext)
    false
  }

  protected def deliverNext =
    row.newWith2(relName, rel, to, other)
}

abstract
class ExpandPipeIteratorForIntTypes(input: Iterator[ExecutionContext],
                                    from: String,
                                    relName: String,
                                    to: String,
                                    dir: Direction,
                                    types: Seq[Int])(implicit qtx: QueryContext)
  extends ExpandPipeIterator(input, from, relName, to, dir) {

  protected def expandNode = qtx.getRelationshipsForIds(node, dir, types)
}

abstract
class ExpandPipeIteratorForStringTypes(input: Iterator[ExecutionContext],
                                       from: String,
                                       relName: String,
                                       to: String,
                                       dir: Direction,
                                       types: Seq[String])(implicit qtx: QueryContext)
  extends ExpandPipeIterator(input, from, relName, to, dir) {

  protected def expandNode = qtx.getRelationshipsFor(node, dir, types)
}
