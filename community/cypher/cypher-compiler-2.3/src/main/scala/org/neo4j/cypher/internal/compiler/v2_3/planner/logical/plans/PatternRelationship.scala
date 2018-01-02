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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, ast}
import org.neo4j.cypher.internal.frontend.v2_3.ast.RelTypeName
import org.neo4j.cypher.internal.frontend.v2_3.perty.PageDocFormatting

final case class PatternRelationship(name: IdName, nodes: (IdName, IdName), dir: SemanticDirection,
                                     types: Seq[RelTypeName], length: PatternLength)
  extends PageDocFormatting { // with ToPrettyString[PatternRelationship] {

//  def toDefaultPrettyString(formatter: DocFormatter) =
//    toPrettyString(formatter)(InternalDocHandler.docGen)

  def directionRelativeTo(node: IdName): SemanticDirection = if (node == left) dir else dir.reversed

  def otherSide(node: IdName) = if (node == left) right else left

  def coveredIds: Set[IdName] = Set(name, left, right)

  def left = nodes._1

  def right = nodes._2

  def inOrder = dir match {
    case SemanticDirection.INCOMING => (right, left)
    case _ => (left, right)
  }
}

object PatternRelationship {
  implicit val byName = Ordering.by { (patternRel: PatternRelationship) => patternRel.name }
}

// TODO: Remove ast representation
final case class ShortestPathPattern(name: Option[IdName], rel: PatternRelationship, single: Boolean)(val expr: ast.ShortestPaths)
  extends PageDocFormatting { // with ToPrettyString[ShortestPathPattern] {

//  def toDefaultPrettyString(formatter: DocFormatter) =
//    toPrettyString(formatter)(InternalDocHandler.docGen)

  def isFindableFrom(symbols: Set[IdName]) = symbols.contains(rel.left) && symbols.contains(rel.right)

  def availableSymbols: Set[IdName] = name.toSet ++ rel.coveredIds
}

object ShortestPathPattern {
  implicit val byRelName = Ordering.by { (sp: ShortestPathPattern) => sp.rel }
}

trait PatternLength extends PageDocFormatting { // with ToPrettyString[PatternLength] {

//  def toDefaultPrettyString(formatter: DocFormatter) =
//    toPrettyString(formatter)(InternalDocHandler.docGen)

  def implicitPatternNodeCount: Int
  def isSimple: Boolean
}

case object SimplePatternLength extends PatternLength {
  def isSimple = true

  def implicitPatternNodeCount: Int = 0
}

final case class VarPatternLength(min: Int, max: Option[Int]) extends PatternLength {
  def isSimple = false

  def implicitPatternNodeCount = max.getOrElse(VarPatternLength.STAR_LENGTH)
}

object VarPatternLength {
  val STAR_LENGTH = 16

  def unlimited = VarPatternLength(1, None)

  def fixed(length: Int) = VarPatternLength(length, Some(length))
}

