/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.Repetition

/**
 * Part of a pattern that is connecting nodes (as in "connected components").
 * This is a generalisation of relationships to be able to plan quantified path patterns using the same algorithm.
 */
sealed trait NodeConnection {

  val left: String
  val right: String

  val nodes: (String, String)

  lazy val coveredNodeIds: Set[String] = Set(left, right)

  def coveredIds: Set[String]

  def otherSide(node: String): String =
    if (node == left) {
      right
    } else if (node == right) {
      left
    } else {
      throw new IllegalArgumentException(
        s"Did not provide either side as an argument to otherSide. Rel: $this, argument: $node"
      )
    }
}

final case class PatternRelationship(
  name: String,
  nodes: (String, String),
  dir: SemanticDirection,
  types: Seq[RelTypeName],
  length: PatternLength
) extends NodeConnection {

  def directionRelativeTo(node: String): SemanticDirection = if (node == left) dir else dir.reversed

  override lazy val coveredIds: Set[String] = Set(name, left, right)

  override val left: String = nodes._1
  override val right: String = nodes._2

  def inOrder: (String, String) = dir match {
    case SemanticDirection.INCOMING => (right, left)
    case _                          => (left, right)
  }

  override def toString: String = {
    val lArrow = if (dir == SemanticDirection.INCOMING) "<" else ""
    val rArrow = if (dir == SemanticDirection.OUTGOING) ">" else ""
    val typesStr =
      if (types.isEmpty) {
        ""
      } else {
        types.map(_.name).mkString(":", "|", "")
      }
    val lengthStr = length match {
      case SimplePatternLength              => ""
      case VarPatternLength(1, None)        => "*"
      case VarPatternLength(x, None)        => s"*$x.."
      case VarPatternLength(min, Some(max)) => s"*$min..$max"
    }
    s"(${nodes._1})$lArrow-[$name$typesStr$lengthStr]-$rArrow(${nodes._2})"
  }
}

object PatternRelationship {

  implicit val byName: Ordering[PatternRelationship] = Ordering.by { patternRel: PatternRelationship =>
    patternRel.name
  }
}

sealed trait PatternLength {
  def isSimple: Boolean
  def intersect(patternLength: PatternLength): PatternLength
}

case object SimplePatternLength extends PatternLength {
  def isSimple = true

  override def intersect(patternLength: PatternLength): PatternLength = SimplePatternLength
}

final case class VarPatternLength(min: Int, max: Option[Int]) extends PatternLength {
  def isSimple = false

  override def intersect(patternLength: PatternLength): PatternLength = patternLength match {
    case VarPatternLength(otherMin, otherMax) =>
      val newMax = Seq(max, otherMax).flatten.reduceOption(_ min _)
      VarPatternLength(min.max(otherMin), newMax)
    case _ => throw new IllegalArgumentException("VarPatternLength may only be intersected with VarPatternLength")
  }
}

object VarPatternLength {
  def unlimited: VarPatternLength = VarPatternLength(1, None)

  def fixed(length: Int): VarPatternLength = VarPatternLength(length, Some(length))
}

/**
 * Describes the connection between two juxtaposed nodes - one inside of a [[QuantifiedPathPattern]]
 * and the other one outside.
 */
case class NodeBinding(inner: String, outer: String) {
  override def toString: String = s"(inner=$inner, outer=$outer)"
}

/**
 * Describes a variable that is exposed from a [[QuantifiedPath]].
 *
 * @param singletonName the name of the singleton variable inside the QuantifiedPath.
 * @param groupName     the name of the group variable exposed outside of the QuantifiedPath.
 */
case class VariableGrouping(singletonName: String, groupName: String) {
  override def toString: String = s"(singletonName=$singletonName, groupName=$groupName)"
}

final case class QuantifiedPathPattern(
  leftBinding: NodeBinding,
  rightBinding: NodeBinding,
  pattern: QueryGraph,
  repetition: Repetition,
  nodeVariableGroupings: Set[VariableGrouping],
  relationshipVariableGroupings: Set[VariableGrouping]
) extends NodeConnection {

  override val left: String = leftBinding.outer
  override val right: String = rightBinding.outer

  override val nodes: (String, String) = (left, right)

  override lazy val coveredIds: Set[String] = coveredNodeIds ++ groupings

  override def toString: String = {
    s"QPP($leftBinding, $rightBinding, $pattern, $repetition, $nodeVariableGroupings, $relationshipVariableGroupings)"
  }

  val dependencies: Set[String] = pattern.dependencies

  val groupings: Set[String] = nodeVariableGroupings.map(_.groupName) ++ relationshipVariableGroupings.map(_.groupName)

  /**
   * A "simple" quantified path pattern is defined as a pattern that could be rewritten to a [[PatternRelationship]] without a loss of information.
   * @return true if this qpp is "simple"
   */
  def isSimple: Boolean = {
    this.nodeVariableGroupings.isEmpty &&
    this.pattern.patternRelationships.size == 1 &&
    this.pattern.selections.isEmpty &&
    this.pattern.quantifiedPathPatterns.isEmpty
  }

}
