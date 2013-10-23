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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import expressions.Expression
import expressions.Identifier._
import org.neo4j.graphdb.Direction
import collection.Seq
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.KeyToken

trait Pattern extends TypeSafe with AstNode[Pattern] {
  def optional: Boolean
  def possibleStartPoints: Seq[(String,CypherType)]
  def relTypes:Seq[String]

  protected def leftArrow(dir: Direction) = if (dir == Direction.INCOMING) "<-" else "-"
  protected def rightArrow(dir: Direction) = if (dir == Direction.OUTGOING) "->" else "-"

  def rewrite( f : Expression => Expression) : Pattern

  def rels:Seq[String]

  def identifiers: Seq[String] = possibleStartPoints.map(_._1)
}

object Pattern {
  def identifiers(patterns: Seq[Pattern]): Set[String] = patterns.flatMap(_.identifiers).toSet
}

object RelationshipPattern {
  def unapply(x: Any): Option[(RelationshipPattern, SingleNode, SingleNode)] = x match {
    case pattern@RelatedTo(left, right, _, _, _, _)                   => Some((pattern, left, right))
    case pattern@VarLengthRelatedTo(_, left, right, _, _, _, _, _, _) => Some((pattern, left, right))
    case pattern@ShortestPath(_, left, right, _, _, _, _, _, _)       => Some((pattern, left, right))
    case _                                                    => None
  }
}

trait RelationshipPattern {
  def left:SingleNode
  def right:SingleNode
  def changeEnds(left: SingleNode = this.left, right: SingleNode = this.right): Pattern
}


object RelatedTo {
  def apply(left: String, right: String, relName: String, relType: String, direction: Direction, optional: Boolean = false) =
    new RelatedTo(SingleNode(left), SingleNode(right), relName, Seq(relType), direction, optional)

  def apply(left: String, right: String, relName: String, types: Seq[String], direction: Direction) =
    new RelatedTo(SingleNode(left), SingleNode(right), relName, types, direction, false)

  def optional(left: String, right: String, relName: String, types: Seq[String], direction: Direction) =
    new RelatedTo(SingleNode(left), SingleNode(right), relName, types, direction, true)
}

object SingleOptionalNode {
  def apply(name:String, labels:Seq[KeyToken]=Seq.empty) = SingleNode(name, labels, optional = true)
}

case class SingleNode(name: String, labels: Seq[KeyToken] = Seq.empty, optional: Boolean = false) extends Pattern {
  def possibleStartPoints = Seq(name -> NodeType())

  def predicate = True()

  def rels = Seq.empty

  def relTypes = Seq.empty

  def rewrite(f: (Expression) => Expression) = SingleNode(name, labels.map(_.typedRewrite[KeyToken](f)), optional)

  def children = Seq.empty

  def symbolTableDependencies = Set.empty

  def throwIfSymbolsMissing(symbols: SymbolTable) {}

  override def toString: String = {
    val namePart = if (notNamed(name)) s"${name.drop(9)}" else name
    val labelPart = if (labels.isEmpty) "" else labels.mkString(":", ":", "")
    val optPart = if(optional) "?" else ""
    "(" + namePart + labelPart + optPart + ")"
  }
}

case class RelatedTo(left: SingleNode,
                     right: SingleNode,
                     relName: String,
                     relTypes: Seq[String],
                     direction: Direction,
                     optional: Boolean) extends Pattern with RelationshipPattern {
  override def toString = left + leftArrow(direction) + relInfo + rightArrow(direction) + right

  private def relInfo: String = {
    var info = relName
    if (optional) info = info + "?"
    if (relTypes.nonEmpty) info = info + ":" + relTypes.mkString("|")
    if (info == "") "" else "[" + info + "]"
  }

  val possibleStartPoints: Seq[(String, MapType)] = left.possibleStartPoints ++ right.possibleStartPoints :+ relName->RelationshipType()

  def rewrite(f: (Expression) => Expression) =
    new RelatedTo(left.rewrite(f), right.rewrite(f), relName, relTypes, direction, optional)

  def rels = Seq(relName)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
  }

  def symbolTableDependencies = Set.empty

  def children = Seq.empty

  def changeEnds(left: SingleNode = this.left, right: SingleNode = this.right): RelatedTo =
    copy(left = left, right = right)
}

abstract class PathPattern extends Pattern with RelationshipPattern {
  def pathName: String

  def cloneWithOtherName(newName: String): PathPattern

  def relIterator: Option[String]
}

object VarLengthRelatedTo {
  def apply(pathName: String, left: String, right: String, minHops: Option[Int], maxHops: Option[Int], relTypes: String,
            direction: Direction, relIterator:Option[String]=None, optional: Boolean = false) =
    new VarLengthRelatedTo(pathName, SingleNode(left), SingleNode(right), minHops, maxHops, Seq(relTypes), direction, relIterator, optional)
}

case class VarLengthRelatedTo(pathName: String,
                              left: SingleNode,
                              right: SingleNode,
                              minHops: Option[Int],
                              maxHops: Option[Int],
                              relTypes: Seq[String],
                              direction: Direction,
                              relIterator: Option[String],
                              optional: Boolean) extends PathPattern {

  override def toString: String = pathName + "=" + left + leftArrow(direction) + relInfo + rightArrow(direction) + right

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

    val relName = relIterator.getOrElse("")
    info = relName + info + hops

    if (info == "") "" else "[" + info + "]"
  }

  def rewrite(f: (Expression) => Expression) =
    new VarLengthRelatedTo(pathName, left.rewrite(f), right.rewrite(f),
      minHops, maxHops, relTypes, direction, relIterator, optional)

  lazy val possibleStartPoints: Seq[(String, AnyType)] =
    left.possibleStartPoints ++
      right.possibleStartPoints :+
      pathName -> PathType()

  def rels = Seq()

  def throwIfSymbolsMissing(symbols: SymbolTable) {}

  def children = Seq.empty

  def changeEnds(left: SingleNode = this.left, right: SingleNode = this.right): VarLengthRelatedTo =
    copy(left = left, right = right)
}

case class ShortestPath(pathName: String,
                        left: SingleNode,
                        right: SingleNode,
                        relTypes: Seq[String],
                        dir: Direction,
                        maxDepth: Option[Int],
                        optional: Boolean,
                        single: Boolean,
                        relIterator: Option[String])
  extends PathPattern {

  override def toString: String = pathName + "=" + algo + "(" + left + leftArrow(dir) + relInfo + rightArrow(dir) + right + ")"

  private def algo = if (single) "singleShortestPath" else "allShortestPath"

  def cloneWithOtherName(newName: String) = copy(pathName = newName)

  def symbolTableDependencies = Set(left.name, right.name)

  private def relInfo: String = {
    var info = "["
    if (optional) info = info + "?"
    if (relTypes.nonEmpty) info = info + ":" + relTypes.mkString("|")
    info = info + "*"
    if (maxDepth.nonEmpty) info = info + ".." + maxDepth.get
    info + "]"
  }

  lazy val possibleStartPoints: Seq[(String, NodeType)] = left.possibleStartPoints ++ right.possibleStartPoints

  def rewrite(f: Expression => Expression) =
    new ShortestPath(pathName, left.rewrite(f), right.rewrite(f), relTypes, dir, maxDepth, optional, single, relIterator)

  def rels = Seq()

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    possibleStartPoints.foreach(p => symbols.evaluateType(p._1, p._2))
  }

  def children = Seq.empty

  def changeEnds(left: SingleNode = this.left, right: SingleNode = this.right): ShortestPath =
    copy(left = left, right = right)
}
