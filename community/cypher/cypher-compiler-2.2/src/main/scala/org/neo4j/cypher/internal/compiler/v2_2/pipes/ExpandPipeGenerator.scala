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

import org.neo4j.cypher.internal.compiler.v2_2.commands.Predicate
import org.neo4j.cypher.internal.compiler.v2_2.pipes.expanders.NodeExpander
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{ExpandAll, ExpandInto, ExpansionMode}
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.{ExecutionContext, InternalException}
import org.neo4j.cypher.internal.helpers.{NothingToDeliver, ReadyToDeliver, DeliveryState, Generator}
import org.neo4j.graphdb.{Direction, Node, Relationship}

object ExpandPipeGenerator {
  def apply(input: Iterator[ExecutionContext],
            from: String, relName: String, to: String, dir: Direction,
            mode: ExpansionMode, optPredicate: Option[Predicate] = None, optional: Boolean = false)
           (nodeExpanderFactory: (Direction, QueryContext) => NodeExpander[Relationship])
           (implicit state: QueryState) = {
    val nodeExpander = nodeExpanderFactory(dir, state.query)

    optPredicate match {
      case None =>
        mode match {
          case ExpandAll if optional =>
            new OptionalExpandPipeGenerator(input, from, relName, to, nodeExpander)
            with ExpandAllPipeGenerator

          case ExpandAll =>
            new RegularExpandPipeGenerator(input, from, relName, to, nodeExpander)
            with ExpandAllPipeGenerator

          case ExpandInto if optional =>
            new OptionalExpandPipeGenerator(input, from, relName, to, nodeExpander)
            with ExpandIntoPipeGenerator

          case ExpandInto =>
            new RegularExpandPipeGenerator(input, from, relName, to, nodeExpander)
            with ExpandIntoPipeGenerator
        }

      case Some(filterPredicate) =>
        mode match {
          case ExpandAll if optional =>
            new OptionalExpandPipeGenerator(input, from, relName, to, nodeExpander)
            with ExpandAllFilteredPipeGenerator {
              val predicate = filterPredicate
            }

          case ExpandAll =>
            new RegularExpandPipeGenerator(input, from, relName, to, nodeExpander)
            with ExpandAllFilteredPipeGenerator {
              val predicate = filterPredicate
            }

          case ExpandInto if optional =>
            new OptionalExpandPipeGenerator(input, from, relName, to, nodeExpander)
            with ExpandIntoFilteredPipeGenerator {
              val predicate = filterPredicate
            }

          case ExpandInto =>
            new RegularExpandPipeGenerator(input, from, relName, to, nodeExpander)
            with ExpandIntoFilteredPipeGenerator {
              val predicate = filterPredicate
            }
        }
    }
  }
}

sealed abstract
class ExpandPipeGenerator extends Generator[ExecutionContext] {

  protected var row: ExecutionContext = null
  protected var node: Node = null
  protected var relationships: Iterator[Relationship] = Iterator.empty

  protected val from: String
  protected val relName: String
  protected val to: String

  protected val query: QueryState

  // see {Regular|Optional}ExpandPipeIterator below

  protected def readyToDeliverNextResult: Boolean

  protected def buildNextResult: ExecutionContext
  protected def zeroNextResult: ExecutionContext

  protected def processNextInputRow(nextRow: ExecutionContext, nextNode: Node, nextRels: Iterator[Relationship]): Unit = {
    row = nextRow
    node = nextNode
    relationships = nextRels
  }
}


sealed abstract
class RegularExpandPipeGenerator(protected val input: Iterator[ExecutionContext],
                                 protected val from: String,
                                 protected val relName: String,
                                 protected val to: String,
                                 protected val expandNode: NodeExpander[Relationship])
                                (implicit val query: QueryState)
  extends ExpandPipeGenerator {

  def fetchNext: DeliveryState = {
    var cont: Boolean = true
    do {
      val hasMoreRels = relationships.hasNext
      if (hasMoreRels) {
        if (readyToDeliverNextResult)
          return ReadyToDeliver
      }
      else {
        cont = fetchNextInputRow
      }
    } while (cont)
    NothingToDeliver
  }

  def deliverNext = buildNextResult

  protected def zeroNextResult: ExecutionContext = row

  protected def fetchNextInputRow: Boolean = {
    while (input.hasNext) {
      val nextRow = input.next()
      nextRow.getOrElse(from, throw new InternalException(s"Expected to find a node at $from but found nothing")) match {
        case nextNode: Node =>
          processNextInputRow(nextRow, nextNode, expandNode(nextNode))
          return true

        case null =>
          // just loop

        case value =>
          throw new InternalException(s"Expected to find a node at $from but found $value instead")
      }
    }
    false
  }
}

sealed abstract
class OptionalExpandPipeGenerator(protected val input: Iterator[ExecutionContext],
                                  protected val from: String,
                                  protected val relName: String,
                                  protected val to: String,
                                  protected val expandNode: NodeExpander[Relationship])
                                 (implicit val query: QueryState)
  extends ExpandPipeGenerator {

  protected var emitNullRow = false

  def fetchNext: DeliveryState = {
    var cont: Boolean = true
    do {
      val hasMoreRels = relationships.hasNext
      if (hasMoreRels) {
        if (readyToDeliverNextResult) {
          emitNullRow = false
          return ReadyToDeliver
        }
      } else {
        if (emitNullRow) {
          return ReadyToDeliver
        }
        cont = fetchNextInputRow
        emitNullRow = true
      }
    } while (cont)
    NothingToDeliver
  }

  def deliverNext = {
    if (emitNullRow) {
      val result = zeroNextResult
      emitNullRow = false
      result
    } else {
      buildNextResult
    }
  }

  protected def zeroNextResult: ExecutionContext = {
    row.put(relName, null)
    row.put(to, null)
    row
  }

  protected def fetchNextInputRow: Boolean = {
    while (input.hasNext) {
      val nextRow = input.next()
      nextRow.getOrElse(from, throw new InternalException(s"Expected to find a node at $from but found nothing")) match {
        case nextNode: Node =>
          processNextInputRow(nextRow, nextNode, expandNode(nextNode))
          return true

        case null =>
          processNextInputRow(nextRow, null, Iterator.empty)
          return true

        case value =>
          throw new InternalException(s"Expected to find a node at $from but found $value instead")
      }
    }
    false
  }
}

trait ExpandAllPipeGenerator {
  self: ExpandPipeGenerator =>

  override protected def readyToDeliverNextResult = true

  override protected def buildNextResult = {
    val rel = relationships.next()
    val other = rel.getOtherNode(node)
    row.newWith2(relName, rel, to, other)
  }
}

trait ExpandAllFilteredPipeGenerator {
  self: ExpandPipeGenerator  =>

  def predicate: Predicate

  override protected def readyToDeliverNextResult: Boolean = {
    val rel = relationships.next()
    val other = rel.getOtherNode(node)
    row.put(relName, rel)
    row.put(to, other)
    predicate.isTrue(row)(query)
  }

  override protected def buildNextResult = {
    row.clone()
  }
}

trait ExpandIntoPipeGenerator extends ExpandPipeGenerator {
  self: ExpandPipeGenerator =>

  private var sibling: Any = null
  private var rel: Relationship = null
  private var other: Node = null

  override protected def readyToDeliverNextResult: Boolean = {
    rel = relationships.next()
    other = rel.getOtherNode(node)
    other == sibling
  }

  override protected def buildNextResult =
    row.newWith2(relName, rel, to, other)

  override protected def zeroNextResult = {
    row.put(relName, null)
    row.put(to, sibling)
    row
  }

  abstract override protected def processNextInputRow(nextRow: ExecutionContext, nextNode: Node, nextRels: Iterator[Relationship]): Unit = {
    super.processNextInputRow(nextRow, nextNode, nextRels)
    sibling = row(to)
  }
}

trait ExpandIntoFilteredPipeGenerator extends ExpandPipeGenerator {
  self: ExpandPipeGenerator =>

  private var sibling: Any = null

  def predicate: Predicate

  override protected def readyToDeliverNextResult: Boolean = {
    val rel = relationships.next()
    val other = rel.getOtherNode(node)
    if (other == sibling) {
      row.put(relName, rel)
      row.put(to, other)
      predicate.isTrue(row)(query)
    } else {
      false
    }
  }

  override protected def buildNextResult =
    row.clone()

  override protected def zeroNextResult = {
    row.put(relName, null)
    row.put(to, sibling)
    row
  }

  abstract override protected def processNextInputRow(nextRow: ExecutionContext, nextNode: Node, nextRels: Iterator[Relationship]): Unit = {
    super.processNextInputRow(nextRow, nextNode, nextRels)
    sibling = row(to)
  }
}
