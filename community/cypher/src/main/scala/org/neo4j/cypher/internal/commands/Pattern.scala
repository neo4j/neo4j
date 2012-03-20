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
package org.neo4j.cypher.internal.commands

import org.neo4j.graphdb.Direction
import java.lang.String
import collection.Seq
import org.neo4j.cypher.internal.symbols.{PathType, RelationshipType, NodeType, Identifier}

abstract class Pattern {
  val optional: Boolean
  val predicate: Predicate
  val possibleStartPoints: Seq[Identifier]

  protected def node(name: String) = if (name.startsWith("  UNNAMED")) "()" else name

  protected def left(dir: Direction) = if (dir == Direction.INCOMING) "<-" else "-"

  protected def right(dir: Direction) = if (dir == Direction.OUTGOING) "->" else "-"

  def rewrite( f : Expression => Expression) : Pattern
}

object RelatedTo {
  def apply(left: String, right: String, relName: String, relType: String, direction: Direction, optional: Boolean = false, predicate: Predicate = True()) =
    new RelatedTo(left, right, relName, Some(relType), direction, optional, predicate)
}

case class RelatedTo(left: String, right: String, relName: String, relType: Option[String], direction: Direction, optional: Boolean, predicate: Predicate) extends Pattern {
  override def toString = node(left) + left(direction) + relInfo + right(direction) + node(right)

  private def relInfo: String = {
    var info = if (relName.startsWith("  UNNAMED")) "" else relName
    if (optional) info = info + "?"
    if (relType.nonEmpty) info = info + ":" + relType.get
    if (info == "") "" else "[" + info + "]"
  }

  val possibleStartPoints: Seq[Identifier] = Seq(Identifier(left, NodeType()), Identifier(right, NodeType()), Identifier(relName, RelationshipType()))

  def rewrite(f: (Expression) => Expression) = new RelatedTo(left,right,relName,relType,direction,optional,predicate.rewrite(f))
}

abstract class PathPattern extends Pattern {
  def pathName: String

  def start: String

  def end: String

  def cloneWithOtherName(newName: String): PathPattern

  def relIterator: Option[String]
}

object VarLengthRelatedTo {
  def apply(pathName: String, start: String, end: String, minHops: Option[Int], maxHops: Option[Int], relType: String, direction: Direction, optional: Boolean = false, predicate: Predicate = True()) =
    new VarLengthRelatedTo(pathName, start, end, minHops, maxHops, Some(relType), direction, None, optional, predicate)
}

case class VarLengthRelatedTo(pathName: String,
                              start: String,
                              end: String,
                              minHops: Option[Int],
                              maxHops: Option[Int],
                              relType: Option[String],
                              direction: Direction,
                              relIterator: Option[String],
                              optional: Boolean,
                              predicate: Predicate) extends PathPattern {

  override def toString: String = pathName + "=" + node(start) + left(direction) + relInfo + right(direction) + node(end)


  def cloneWithOtherName(newName: String) = VarLengthRelatedTo(newName, start, end, minHops, maxHops, relType, direction, relIterator, optional, predicate)

  private def relInfo: String = {
    var info = if (optional) "?" else ""
    if (relType.nonEmpty) info = info + ":" + relType.get
    val hops = (minHops, maxHops) match {
      case (None, None) => "*"
      case (Some(min), None) => "*" + min + ".."
      case (None, Some(max)) => "*" + ".." + max
      case (Some(min), Some(max)) => "*" + min + ".." + max
    }

    info = info + hops

    if (info == "") "" else "[" + info + "]"
  }

  lazy val possibleStartPoints: Seq[Identifier] = Seq(Identifier(start, NodeType()), Identifier(end, NodeType()), Identifier(pathName, PathType()) )

  def rewrite(f: (Expression) => Expression) = new VarLengthRelatedTo(pathName,start,end, minHops,maxHops,relType,direction,relIterator,optional,predicate.rewrite(f))
}

case class ShortestPath(pathName: String,
                        start: String,
                        end: String,
                        relType: Option[String],
                        dir: Direction,
                        maxDepth: Option[Int],
                        optional: Boolean,
                        single: Boolean,
                        relIterator: Option[String],
                        predicate: Predicate = True())
  extends PathPattern {
  override def toString: String = pathName + "=" + algo + "(" + start + left(dir) + relInfo + right(dir) + end + ")"

  private def algo = if (single) "singleShortestPath" else "allShortestPath"
  
  def dependencies: Seq[Identifier] = Seq(Identifier(start, NodeType()),Identifier(end, NodeType())) ++ predicate.dependencies

  def cloneWithOtherName(newName: String) = ShortestPath(newName, start, end, relType, dir, maxDepth, optional, single, None)

  private def relInfo: String = {
    var info = "["
    if (optional) info = info + "?"
    if (relType.nonEmpty) info = info + ":" + relType.get
    info = info + "*"
    if (maxDepth.nonEmpty) info = info + ".." + maxDepth.get
    info + "]"
  }

  lazy val possibleStartPoints: Seq[Identifier] = Seq(Identifier(start, NodeType()), Identifier(end, NodeType()))

  def rewrite(f: (Expression) => Expression) = new ShortestPath(pathName,start,end,relType,dir,maxDepth,optional,single,relIterator,predicate.rewrite(f))
}