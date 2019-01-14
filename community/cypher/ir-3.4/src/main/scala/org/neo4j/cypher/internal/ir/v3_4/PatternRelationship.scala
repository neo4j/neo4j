/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ir.v3_4

import org.neo4j.cypher.internal.v3_4.expressions.{RelTypeName, SemanticDirection}

final case class PatternRelationship(name: String, nodes: (String, String), dir: SemanticDirection,
                                     types: Seq[RelTypeName], length: PatternLength) {

  def directionRelativeTo(node: String): SemanticDirection = if (node == left) dir else dir.reversed

  def otherSide(node: String) = if (node == left) right else left

  def coveredIds: Set[String] = Set(name, left, right)

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

trait PatternLength {
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
