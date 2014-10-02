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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_2.ast
import org.neo4j.cypher.internal.compiler.v2_2.ast.RelTypeName
import org.neo4j.cypher.internal.compiler.v2_2.docgen.InternalDocHandler
import org.neo4j.cypher.internal.compiler.v2_2.perty.PageDocFormatting
import org.neo4j.graphdb.Direction

final case class PatternRelationship(name: IdName, nodes: (IdName, IdName), dir: Direction, types: Seq[RelTypeName], length: PatternLength) {
//  extends InternalDocHandler.ToString[PatternRelationship] with PageDocFormatting {

  def directionRelativeTo(node: IdName): Direction = if (node == left) dir else dir.reverse()

  def otherSide(node: IdName) = if (node == left) right else left

  def coveredIds: Set[IdName] = Set(name, left, right)

  def left = nodes._1

  def right = nodes._2

  def inOrder = dir match {
    case Direction.INCOMING => (right, left)
    case _ => (left, right)
  }
}

// TODO: Remove ast representation
final case class ShortestPathPattern(name: Option[IdName], rel: PatternRelationship, single: Boolean)(val expr: ast.ShortestPaths) {
//  extends InternalDocHandler.ToString[ShortestPathPattern] with PageDocFormatting {

  def isFindableFrom(symbols: Set[IdName]) = symbols.contains(rel.left) && symbols.contains(rel.right)

  def availableSymbols: Set[IdName] = name.toSet ++ rel.coveredIds
}


trait PatternLength { // extends InternalDocHandler.ToString[PatternLength] with PageDocFormatting {
  def implicitPatternNodeCount: Int
  def isSimple: Boolean
}

case object SimplePatternLength extends PatternLength {
  def isSimple = true

  def implicitPatternNodeCount: Int = 0
}

final case class VarPatternLength(min: Int, max: Option[Int]) extends PatternLength {
  def isSimple = false

  def implicitPatternNodeCount = max.getOrElse(15)
}

object VarPatternLength {
  def unlimited = VarPatternLength(1, None)

  def fixed(length: Int) = VarPatternLength(length, Some(length))
}

