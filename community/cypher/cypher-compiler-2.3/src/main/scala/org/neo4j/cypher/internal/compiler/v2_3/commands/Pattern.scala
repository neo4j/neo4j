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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.True
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.compiler.v2_3.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.compiler.v2_3.symbols.TypeSafe
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.{INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

import scala.collection.{Map, Seq}
trait Pattern extends TypeSafe with AstNode[Pattern] {
  def possibleStartPoints: Seq[(String,CypherType)]
  def relTypes:Seq[String]

  protected def leftArrow(dir: SemanticDirection) = if (dir == INCOMING) "<-" else "-"
  protected def rightArrow(dir: SemanticDirection) = if (dir == OUTGOING) "->" else "-"

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
    case pattern@ShortestPath(_, left, right, _, _, _, _, _, _)          => Some((pattern, left, right))
    case _                                                            => None
  }
}

trait RelationshipPattern {
  def left:SingleNode
  def right:SingleNode
  def changeEnds(left: SingleNode = this.left, right: SingleNode = this.right): Pattern
}

case class SingleNode(name: String,
                      labels: Seq[KeyToken] = Seq.empty,
                      properties: Map[String, Expression]=Map.empty) extends Pattern with GraphElementPropertyFunctions {
  def possibleStartPoints = Seq(name -> CTNode)

  def predicate = True()

  def rels = Seq.empty

  def relTypes = Seq.empty

  def rewrite(f: (Expression) => Expression) = SingleNode(name, labels.map(_.typedRewrite[KeyToken](f)), properties.rewrite(f))

  def children = Seq.empty

  def symbolTableDependencies = properties.symboltableDependencies

  override def toString: String = {
    val namePart = if (UnNamedNameGenerator.notNamed(name)) s"${name.drop(9)}" else name
    val labelPart = if (labels.isEmpty) "" else labels.mkString(":", ":", "")
    val props = if (properties.isEmpty) "" else " " + toString(properties)
    "(%s%s%s)".format(namePart, labelPart, props)
  }
}

object RelatedTo {
  def apply(left: String, right: String, relName: String, relType: String, direction: SemanticDirection) =
    new RelatedTo(SingleNode(left), SingleNode(right), relName, Seq(relType), direction, Map.empty)

  def apply(left: String, right: String, relName: String, types: Seq[String], direction: SemanticDirection) =
    new RelatedTo(SingleNode(left), SingleNode(right), relName, types, direction, Map.empty)
}

case class RelatedTo(left: SingleNode,
                     right: SingleNode,
                     relName: String,
                     relTypes: Seq[String],
                     direction: SemanticDirection,
                     properties: Map[String, Expression])
  extends Pattern with RelationshipPattern with GraphElementPropertyFunctions {
  override def toString = left + leftArrow(direction) + relInfo + rightArrow(direction) + right

  private def relInfo: String = {
    var info = relName
    if (relTypes.nonEmpty) info += ":" + relTypes.mkString("|")
    if (properties.nonEmpty) info += toString(properties)

    if (info == "") "" else "[" + info + "]"
  }

  val possibleStartPoints: Seq[(String, CypherType)] = left.possibleStartPoints ++ right.possibleStartPoints :+ relName->CTRelationship

  def rewrite(f: (Expression) => Expression) =
    new RelatedTo(left.rewrite(f), right.rewrite(f), relName, relTypes, direction, properties.rewrite(f))

  def rels = Seq(relName)

  def symbolTableDependencies =
      properties.symboltableDependencies ++
      left.symbolTableDependencies ++
      right.symbolTableDependencies

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
            direction: SemanticDirection, relIterator:Option[String]=None) =
    new VarLengthRelatedTo(pathName, SingleNode(left), SingleNode(right), minHops, maxHops, Seq(relTypes), direction, relIterator, Map.empty)
}

case class VarLengthRelatedTo(pathName: String,
                              left: SingleNode,
                              right: SingleNode,
                              minHops: Option[Int],
                              maxHops: Option[Int],
                              relTypes: Seq[String],
                              direction: SemanticDirection,
                              relIterator: Option[String],
                              properties: Map[String, Expression]) extends PathPattern with GraphElementPropertyFunctions {

  override def toString: String = pathName + "=" + left + leftArrow(direction) + relInfo + rightArrow(direction) + right

  def symbolTableDependencies =
    properties.symboltableDependencies ++
      left.symbolTableDependencies ++
      right.symbolTableDependencies

  def cloneWithOtherName(newName: String) = copy(pathName = newName)

  private def relInfo: String = {
    var info = relTypes.mkString("|")
    val hops = (minHops, maxHops) match {
      case (None, None)           => "*"
      case (Some(min), None)      => "*" + min + ".."
      case (None, Some(max))      => "*" + ".." + max
      case (Some(min), Some(max)) => "*" + min + ".." + max
    }
    if (properties.nonEmpty) info += toString(properties)

    val relName = relIterator.getOrElse("")
    info = relName + info + hops

    if (info == "") "" else "[" + info + "]"
  }

  def rewrite(f: (Expression) => Expression) =
    new VarLengthRelatedTo(pathName, left.rewrite(f), right.rewrite(f),
      minHops, maxHops, relTypes, direction, relIterator, properties.rewrite(f))

  lazy val possibleStartPoints: Seq[(String, CypherType)] =
    left.possibleStartPoints ++
      right.possibleStartPoints :+
      pathName -> CTPath

  def rels = Seq()

  def children = Seq.empty

  def changeEnds(left: SingleNode = this.left, right: SingleNode = this.right): VarLengthRelatedTo =
    copy(left = left, right = right)
}

case class ShortestPath(pathName: String,
                        left: SingleNode,
                        right: SingleNode,
                        relTypes: Seq[String],
                        dir: SemanticDirection,
                        allowZeroLength: Boolean,
                        maxDepth: Option[Int],
                        single: Boolean,
                        relIterator: Option[String])
  extends PathPattern {

  override def toString: String = pathName + "=" + algo + "(" + left + leftArrow(dir) + relInfo + rightArrow(dir) + right + ")"

  private def algo = if (single) "singleShortestPath" else "allShortestPath"

  def cloneWithOtherName(newName: String) = copy(pathName = newName)

  def symbolTableDependencies =
      Set(left.name, right.name)

  private def relInfo: String = {
    var info = "["
    if (relTypes.nonEmpty) info += ":" + relTypes.mkString("|")
    info += "*"
    if (allowZeroLength) info += "0"
    if (allowZeroLength || maxDepth.nonEmpty) info += ".."
    if (maxDepth.nonEmpty) info += maxDepth.get
    info + "]"
  }

  lazy val possibleStartPoints: Seq[(String, NodeType)] = left.possibleStartPoints ++ right.possibleStartPoints

  def rewrite(f: Expression => Expression) =
    new ShortestPath(pathName, left.rewrite(f), right.rewrite(f), relTypes, dir, allowZeroLength, maxDepth, single, relIterator)

  def rels = Seq()

  def children = Seq.empty

  def changeEnds(left: SingleNode = this.left, right: SingleNode = this.right): ShortestPath =
    copy(left = left, right = right)
}
