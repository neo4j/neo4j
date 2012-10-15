/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.graphdb.{Node, Relationship, Direction, RelationshipType}
import collection.mutable
import collection.JavaConverters._
import org.neo4j.cypher.internal.commands.{True, Predicate}
import collection.Map
import org.neo4j.cypher.internal.pipes.ExecutionContext

case class ExpanderStep(id: Int,
                        typ: Seq[RelationshipType],
                        direction: Direction,
                        next: Option[ExpanderStep],
                        relPredicate: Predicate,
                        nodePredicate: Predicate) {
  def reverse(): ExpanderStep = {
    val allSteps = getAllStepsAsSeq()

    val reversed = allSteps.foldLeft[Option[ExpanderStep]]((None)) {
      case (last, step) =>
        val p = step.next.map(_.nodePredicate).getOrElse(True())
        Some(step.copy(next = last, direction = step.direction.reverse(), nodePredicate = p))
    }

    assert(reversed.nonEmpty, "The reverse of an expander should never be empty")

    reversed.get
  }

  def filter(r: Relationship, n: Node, parameters: ExecutionContext): Boolean = {
    val m = new MiniMap(r, n, parameters)
    relPredicate.isMatch(m) && nodePredicate.isMatch(m)
  }

  def expand(node: Node, parameters: ExecutionContext): Iterable[Relationship] = typ match {
    case Seq() => node.getRelationships(direction).asScala.filter(r => filter(r, r.getOtherNode(node), parameters))
    case x     => node.getRelationships(direction, x: _*).asScala.filter(r => filter(r, r.getOtherNode(node), parameters))
  }

  private def getAllStepsAsSeq(): Seq[ExpanderStep] = {
    var allSteps = mutable.Seq[ExpanderStep]()
    var current: Option[ExpanderStep] = Some(this)

    while (current.nonEmpty) {
      val step = current.get
      allSteps = allSteps :+ step
      current = step.next
    }

    allSteps.toSeq
  }

  private def shape = "(%s)%s-%s-%s".format(id, left, relInfo, right)

  private def left =
    if (direction == Direction.OUTGOING)
      ""
    else
      "<"

  private def right =
    if (direction == Direction.INCOMING)
      ""
    else
      ">"

  private def relInfo = typ.toList match {
    case List() => ""
    case _      => "[:%s {%s,%s}]".format(typ.map(_.name()).mkString("|"), relPredicate, nodePredicate)
  }

  override def toString = next match {
    case None    => "%s()".format(shape)
    case Some(x) => shape + x.toString
  }

  override def equals(p1: Any) = p1 match {
    case null                => false
    case other: ExpanderStep =>
      val a = id == other.id
      val b = direction == other.direction
      val c = next == other.next
      val d = typ.map(_.name()) == other.typ.map(_.name())
      val e = relPredicate == other.relPredicate
      val f = nodePredicate == other.nodePredicate
      a && b && c && d && e && f
    case _                   => false
  }

  def size: Int = next match {
    case Some(s) => 1 + s.size
    case None    => 1
  }
}

class MiniMap(r: Relationship, n: Node, parameters: ExecutionContext)
  extends ExecutionContext(params = parameters.params) {
  override def get(key: String): Option[Any] =
    if (key == "r")
      Some(r)
    else if (key == "n")
      Some(n)
    else
      parameters.get(key)

  override def iterator = throw new RuntimeException

  override def -(key: String) = throw new RuntimeException

  override def +[B1 >: Any](kv: (String, B1)) = throw new RuntimeException

  override def newWith(newEntries: Seq[(String, Any)]) = throw new RuntimeException

  override def newWith(newEntries: scala.collection.Map[String, Any]) = throw new RuntimeException

  override def newFrom(newEntries: Seq[(String, Any)]) = throw new RuntimeException

  override def newFrom(newEntries: scala.collection.Map[String, Any]) = throw new RuntimeException

  override def newWith(newEntry: (String, Any)) = throw new RuntimeException
}
