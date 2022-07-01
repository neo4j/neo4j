/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

object Pattern {

  sealed trait SemanticContext {
    def name: String = SemanticContext.name(this)
  }

  object SemanticContext {
    case object Match extends SemanticContext
    case object Merge extends SemanticContext
    case object Create extends SemanticContext
    case object Expression extends SemanticContext

    def name(ctx: SemanticContext): String = ctx match {
      case Match      => "MATCH"
      case Merge      => "MERGE"
      case Create     => "CREATE"
      case Expression => "expression"
    }
  }
}

/**
 * Represents a comma-separated list of pattern parts. Therefore, this is known in the parser as PatternList.
 * As we (in contrast to PatternParts) can use this to describe arbitrary shaped graph structures, GQL refers to these as graph patterns.
 */
case class Pattern(patternParts: Seq[PatternPart])(val position: InputPosition) extends ASTNode {

  lazy val length: Int = this.folder.fold(0) {
    case RelationshipChain(_, _, _) => _ + 1
    case _                          => identity
  }
}

case class RelationshipsPattern(element: RelationshipChain)(val position: InputPosition) extends ASTNode

/**
 * Represents one part in the comma-separated list of a pattern.
 *
 * In the parser, this is just referred to as Pattern.
 * As we (in contrast to pattern lists) can only use this to describe linear graph structures, GQL refers to these as path patterns.
 */
sealed abstract class PatternPart extends ASTNode {
  def allVariables: Set[LogicalVariable]
  def element: PatternElement
}

case class NamedPatternPart(variable: Variable, patternPart: AnonymousPatternPart)(val position: InputPosition)
    extends PatternPart {
  override def element: PatternElement = patternPart.element

  override def allVariables: Set[LogicalVariable] = patternPart.allVariables + variable
}

sealed trait AnonymousPatternPart extends PatternPart {
  override def allVariables: Set[LogicalVariable] = element.allVariables
}

case class EveryPath(element: PatternElement) extends AnonymousPatternPart {
  override def position: InputPosition = element.position
}

case class ShortestPaths(element: PatternElement, single: Boolean)(val position: InputPosition)
    extends AnonymousPatternPart {

  val name: String =
    if (single)
      "shortestPath"
    else
      "allShortestPaths"
}

/**
 * Contains a list of elements that are concatenated in the query.
 *
 * NOTE that the concatenation is recorded only in the order of the factors in the sequence.
 * That is that `factors(i)` is concatenated with `factors(i - 1)` and `factors(i + 1)` if they exist.
 */
case class PathConcatenation(factors: Seq[PathFactor])(val position: InputPosition) extends PatternElement {
  override def allVariables: Set[LogicalVariable] = factors.flatMap(_.allVariables).toSet

  override def variable: Option[LogicalVariable] = None
}

/**
 * Elements of this trait can be put next to each other to form a juxtaposition in a pattern.
 */
sealed trait PathFactor extends PatternElement

sealed trait PatternAtom extends ASTNode

case class QuantifiedPath(
  part: PatternPart,
  quantifier: GraphPatternQuantifier
)(val position: InputPosition)
    extends PathFactor with PatternAtom {

  override def allVariables: Set[LogicalVariable] = part.allVariables

  override def variable: Option[LogicalVariable] = None
}

// We can currently parse these but not plan them. Therefore, we represent them in the AST but disallow them in semantic checking when concatenated and unwrap them otherwise.
case class ParenthesizedPath(
  part: PatternPart
)(val position: InputPosition)
    extends PathFactor with PatternAtom {

  override def allVariables: Set[LogicalVariable] = part.element.allVariables

  override def variable: Option[LogicalVariable] = None
}

sealed abstract class PatternElement extends ASTNode {
  def allVariables: Set[LogicalVariable]
  def variable: Option[LogicalVariable]

  def isSingleNode = false
}

/**
 * A part of the pattern that consists of alternating nodes and relationships, starting and ending in a node.
 */
sealed abstract class SimplePattern extends PathFactor

case class RelationshipChain(
  element: SimplePattern,
  relationship: RelationshipPattern,
  rightNode: NodePattern
)(val position: InputPosition)
    extends SimplePattern {

  override def variable: Option[LogicalVariable] = relationship.variable

  override def allVariables: Set[LogicalVariable] = element.allVariables ++ relationship.variable ++ rightNode.variable

}

object RelationshipChain {

  /**
   * This method will traverse into any ASTNode and find duplicate relationship variables inside of RelationshipChains.
   *
   * For each rel variable that is duplicated, return the first occurrence of that variable.
   */
  def findDuplicateRelationships(treeNode: ASTNode): Seq[LogicalVariable] = {
    val duplicates = treeNode.folder.fold(Map[String, List[LogicalVariable]]().withDefaultValue(Nil)) {
      case RelationshipChain(_, RelationshipPattern(Some(rel), _, None, _, _, _), _) =>
        map =>
          map.updated(rel.name, rel :: map(rel.name))
      case _ =>
        identity
    }
    duplicates.values.filter(_.size > 1).map(_.minBy(_.position)).toSeq
  }
}

/**
 * Represents one node in a pattern.
 */
case class NodePattern(
  variable: Option[LogicalVariable],
  labelExpression: Option[LabelExpression],
  properties: Option[Expression],
  predicate: Option[Expression]
)(val position: InputPosition)
    extends SimplePattern with PatternAtom {

  override def allVariables: Set[LogicalVariable] = variable.toSet

  override def isSingleNode = true
}

/**
 * Represents one relationship (without its neighbouring nodes) in a pattern.
 */
case class RelationshipPattern(
  variable: Option[LogicalVariable],
  labelExpression: Option[LabelExpression],
  length: Option[Option[Range]],
  properties: Option[Expression],
  predicate: Option[Expression],
  direction: SemanticDirection
)(val position: InputPosition) extends ASTNode with PatternAtom {

  def isSingleLength: Boolean = length.isEmpty

  def isDirected: Boolean = direction != SemanticDirection.BOTH
}
