/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import expressions.Expression
import expressions.Identifier._
import org.neo4j.graphdb.Direction
import collection.Seq
import org.neo4j.cypher.internal.symbols._

trait Pattern extends TypeSafe with AstNode[Pattern] {
  def optional: Boolean
  def possibleStartPoints: Seq[(String,CypherType)]
  def relTypes:Seq[String]

  protected def node(name: String) = if (notNamed(name)) s"(${name.drop(9)})" else name
  protected def leftArrow(dir: Direction) = if (dir == Direction.INCOMING) "<-" else "-"
  protected def rightArrow(dir: Direction) = if (dir == Direction.OUTGOING) "->" else "-"

  def rewrite( f : Expression => Expression) : Pattern
  protected def filtered(x:Seq[String]): Seq[String] =x.filter(isNamed)

  def nodes:Seq[String]
  def rels:Seq[String]
}

object RelatedTo {
  def apply(left: String, right: String, relName: String, relType: String, direction: Direction, optional: Boolean = false) =
    new RelatedTo(left, right, relName, Seq(relType), direction, optional)
}

case class RelatedTo(left: String,
                     right: String,
                     relName: String,
                     relTypes: Seq[String],
                     direction: Direction,
                     optional: Boolean) extends Pattern {
  override def toString = node(left) + leftArrow(direction) + relInfo + rightArrow(direction) + node(right)

  private def relInfo: String = {
    var info = relName
    if (optional) info = info + "?"
    if (relTypes.nonEmpty) info = info + ":" + relTypes.mkString("|")
    if (info == "") "" else "[" + info + "]"
  }

  val possibleStartPoints: Seq[(String, MapType)] = Seq(left-> NodeType(), right-> NodeType(), relName->RelationshipType())

  def rewrite(f: (Expression) => Expression) =
    new RelatedTo(left, right, relName, relTypes, direction, optional)

  def nodes = Seq(left,right)

  def rels = Seq(relName)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
  }

  def symbolTableDependencies = Set.empty

  def children = Seq.empty

  override def addsToRow() = Seq(left, right, relName)
}

abstract class PathPattern extends Pattern {
  def pathName: String

  def start: String

  def end: String

  def cloneWithOtherName(newName: String): PathPattern

  def relIterator: Option[String]
}

object VarLengthRelatedTo {
  def apply(pathName: String, start: String, end: String, minHops: Option[Int], maxHops: Option[Int], relTypes: String, direction: Direction, optional: Boolean = false) =
    new VarLengthRelatedTo(pathName, start, end, minHops, maxHops, Seq(relTypes), direction, None, optional)
}

case class VarLengthRelatedTo(pathName: String,
                              start: String,
                              end: String,
                              minHops: Option[Int],
                              maxHops: Option[Int],
                              relTypes: Seq[String],
                              direction: Direction,
                              relIterator: Option[String],
                              optional: Boolean) extends PathPattern {

  override def toString: String = pathName + "=" + node(start) + leftArrow(direction) + relInfo + rightArrow(direction) + node(end)

  def symbolTableDependencies = Set.empty

  def cloneWithOtherName(newName: String) = copy(pathName = newName)

  private def relInfo: String = {
    var info = if (optional) "?" else ""
    if (relTypes.nonEmpty) info = info + ":" + relTypes.mkString("|")
    val hops = (minHops, maxHops) match {
      case (None, None) => "*"
      case (Some(min), None) => "*" + min + ".."
      case (None, Some(max)) => "*" + ".." + max
      case (Some(min), Some(max)) => "*" + min + ".." + max
    }

    info = info + hops

    if (info == "") "" else "[" + info + "]"
  }

  def rewrite(f: (Expression) => Expression) = new VarLengthRelatedTo(pathName,start,end, minHops,maxHops,relTypes,direction,relIterator,optional)
  lazy val possibleStartPoints: Seq[(String, AnyType)] = Seq(start -> NodeType(), end -> NodeType(), pathName -> PathType())

  def nodes = Seq(start,end)

  def rels = Seq()

  def throwIfSymbolsMissing(symbols: SymbolTable) {
  }

  def children = Seq.empty
}

case class ShortestPath(pathName: String,
                        start: String,
                        end: String,
                        relTypes: Seq[String],
                        dir: Direction,
                        maxDepth: Option[Int],
                        optional: Boolean,
                        single: Boolean,
                        relIterator: Option[String])
  extends PathPattern {
  override def toString: String = pathName + "=" + algo + "(" + start + leftArrow(dir) + relInfo + rightArrow(dir) + end + ")"

  private def algo = if (single) "singleShortestPath" else "allShortestPath"

  def cloneWithOtherName(newName: String) = copy(pathName = newName)

  def symbolTableDependencies = Set(start, end)

  private def relInfo: String = {
    var info = "["
    if (optional) info = info + "?"
    if (relTypes.nonEmpty) info = info + ":" + relTypes.mkString("|")
    info = info + "*"
    if (maxDepth.nonEmpty) info = info + ".." + maxDepth.get
    info + "]"
  }

  lazy val possibleStartPoints: Seq[(String, NodeType)] = Seq(start-> NodeType(), end-> NodeType())

  def rewrite(f: Expression => Expression) = new ShortestPath(pathName,start,end,relTypes,dir,maxDepth,optional,single,relIterator)

  def rels = Seq()

  def nodes = Seq(start,end)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    possibleStartPoints.foreach(p => symbols.evaluateType(p._1, p._2))
  }

  def children = Seq.empty
}