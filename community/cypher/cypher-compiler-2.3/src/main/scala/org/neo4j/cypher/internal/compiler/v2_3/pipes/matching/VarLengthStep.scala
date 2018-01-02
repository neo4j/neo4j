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

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.helpers.DynamicIterable
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{LazyTypes, QueryState}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.graphdb.{Node, Relationship}

/*
Variable length paths are expanded by decreasing min and max, if it's a bounded path. Once
min reaches 0, the next step is also given a chance to find relationships. Since this
is done recursively, many steps could in theory be involved in the expansion of this single
step.

Once max reaches 1, the next step will be what is stored in the `next` field.

Finally, if we don't find any matching relationships, we either allow our next step to
expand, if we have one. If we don't have a next step, we return an empty result and None
as the next step.
 */
case class VarLengthStep(id: Int,
                         typ: Seq[String],
                         direction: SemanticDirection,
                         min: Int,
                         max: Option[Int],
                         next: Option[ExpanderStep],
                         relPredicate: Predicate,
                         nodePredicate: Predicate) extends ExpanderStep {
  def createCopy(next: Option[ExpanderStep], direction: SemanticDirection, nodePredicate: Predicate): ExpanderStep =
    copy(next = next, direction = direction, nodePredicate = nodePredicate)

  private val types = LazyTypes(typ)

  def expand(node: Node, parameters: ExecutionContext, state: QueryState): (Iterable[Relationship], Option[ExpanderStep]) = {
    def filter(r: Relationship, n: Node): Boolean = {
      val m = new MiniMap(r, n)
      relPredicate.isTrue(m)(state) && nodePredicate.isTrue(m)(state)
    }

    def decrease(v: Option[Int]): Option[Int] = v.map {
      case 0 => 0
      case x => x - 1
    }

    def forceNextStep() = next match {
      case None       => (Seq(), None)
      case Some(step) => step.expand(node, parameters, state)
    }

    def expandRecursively(rels: Iterable[Relationship]): Iterable[Relationship] = {
      if (min == 0) {
        rels ++ next.toSeq.map(s => s.expand(node, parameters, state)._1).flatten
      } else {
        rels
      }
    }

    def decreaseAndReturnNewNextStep(): Option[ExpanderStep] = {
      if (max == Some(1)) {
        next
      } else {
        val newMax = decrease(max)
        Some(copy(min = if (min == 0) 0 else min - 1, max = newMax))
      }
    }

    val matchingRelationships = DynamicIterable( state.query.getRelationshipsForIds(node, direction, types.types(state.query)) )


    val result = if (matchingRelationships.isEmpty && min == 0) {
      /*
      If we didn't find any matching relationships, and min is zero, we'll strip away the current step, and keep
      the next step
       */
      forceNextStep()
    } else {
      /*
      If min is not zero, we'll return whatever we found, decrease and return this step
      */
      (expandRecursively(matchingRelationships), decreaseAndReturnNewNextStep())
    }
    result
  }

  def size: Option[Int] = next match {
    case None                    => max
    case Some(n) if max.nonEmpty => n.size.map(_ + max.get)
    case _                       => None
  }

  override def toString = {
    val predicateString = "r: %s, n: %s".format(relPredicate, nodePredicate)

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

    val typeString =
      typ.mkString("|")

    val varLengthString = max match {
      case None    => "%s..".format(min)
      case Some(y) => "%s..%s".format(min, y)
    }

    val shape = "(%s)%s-[:%s*%s {%s}]-%s".format(id, left, typeString, varLengthString, predicateString, right)

    next match {
      case None    => "%s()".format(shape)
      case Some(x) => shape + x.toString
    }
  }

  override def equals(p1: Any) = p1 match {
    case null                 => false
    case other: VarLengthStep =>
      val a = id == other.id
      val b = direction == other.direction
      val c = typ == other.typ
      val d = min == other.min
      val e = max == other.max
      val f = next == other.next
      val g = relPredicate == other.relPredicate
      val h = nodePredicate == other.nodePredicate
      a && b && c && d && e && f && g && h
    case _                    => false
  }

  def shouldInclude() = min == 0 && next.forall(_.shouldInclude())
}
