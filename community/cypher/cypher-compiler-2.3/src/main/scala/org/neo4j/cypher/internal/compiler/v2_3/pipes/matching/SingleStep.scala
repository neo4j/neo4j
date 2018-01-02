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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import SingleStep.FilteringIterator
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{True, And, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.DynamicIterable
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{LazyTypes, QueryState}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.graphdb.{Node, Relationship}

case class SingleStep(id: Int,
                      typ: Seq[String],
                      direction: SemanticDirection,
                      next: Option[ExpanderStep],
                      relPredicate: Predicate,
                      nodePredicate: Predicate) extends ExpanderStep {

  def createCopy(next: Option[ExpanderStep], direction: SemanticDirection, nodePredicate: Predicate): ExpanderStep =
    copy(next = next, direction = direction, nodePredicate = nodePredicate)

  private val combinedPredicate: Predicate = And(relPredicate, nodePredicate)
  private val needToFilter = combinedPredicate != True()
  private val types = LazyTypes(typ)

  def expand(node: Node, parameters: ExecutionContext, state: QueryState): (Iterable[Relationship], Option[ExpanderStep]) = {
    val rels = DynamicIterable {
      val allRelationships = state.query.getRelationshipsForIds(node, direction, types.types(state.query))
      if (needToFilter) FilteringIterator( node, combinedPredicate, state, allRelationships ) else allRelationships
    }
    (rels, next)
  }

  override def toString = {
    val left =
      if (direction == SemanticDirection.OUTGOING)
        ""
      else
        "<"

    val right =
      if (direction == SemanticDirection.INCOMING)
        ""
      else
        ">"

    val relInfo = typ.toList match {
      case List() => "[{%s,%s}]".format(relPredicate, nodePredicate)
      case _      => "[:%s {%s,%s}]".format(typ.mkString("|"), relPredicate, nodePredicate)
    }

    val shape = "(%s)%s-%s-%s".format(id, left, relInfo, right)

    next match {
      case None    => "%s()".format(shape)
      case Some(x) => shape + x.toString
    }
  }

  def size: Option[Int] = next match {
    case None    => Some(1)
    case Some(n) => n.size.map(_ + 1)
  }

  override def equals(p1: Any) = p1 match {
    case null                => false
    case other: ExpanderStep =>
      val a = id == other.id
      val b = direction == other.direction
      val c = next == other.next
      val d = typ == other.typ
      val e = relPredicate == other.relPredicate
      val f = nodePredicate == other.nodePredicate
      a && b && c && d && e && f
    case _                   => false
  }

  def shouldInclude() = false
}

object SingleStep {
  final case class FilteringIterator(startNode: Node, predicate: Predicate, state: QueryState,
                                     inner: Iterator[Relationship]) extends Iterator[Relationship] {
    val miniMap: MiniMap = new MiniMap(null, startNode)
    var _next: Relationship = computeNext()

    def hasNext: Boolean = _next != null

    def next(): Relationship = {
      if (hasNext) {
        val result = _next
        _next = computeNext()
        result
      } else {
        throw new NoSuchElementException
      }
    }

    private def computeNext(): Relationship = {
      while (inner.hasNext) {
        val nextCandidate = asValidNextCandidate(inner.next())
        if (isValidNext(nextCandidate)) {
          return nextCandidate
        }
      }
      null
    }

    private def asValidNextCandidate(r: Relationship): Relationship = {
      if (null == r)
        throw new IllegalStateException("Inner iterator delivered null as Relationship")
      r
    }

    private def isValidNext(r: Relationship): Boolean = {
      miniMap.relationship = r
      miniMap.node = r.getOtherNode(startNode)

      predicate.isTrue(miniMap)(state)
    }
  }
}
