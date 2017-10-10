/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.util.v3_4.{ASTNode, InputPosition}

object Pattern {
  sealed trait SemanticContext

  object SemanticContext {
    case object Match extends SemanticContext
    case object Merge extends SemanticContext
    case object Create extends SemanticContext
    case object CreateUnique extends SemanticContext
    case object Expression extends SemanticContext

    case object GraphOf extends SemanticContext

    def name(ctx: SemanticContext): String = ctx match {
      case Match => "MATCH"
      case Merge => "MERGE"
      case Create => "CREATE"
      case CreateUnique => "CREATE UNIQUE"
      case Expression => "expression"
      case GraphOf => "GRAPH OF"
    }
  }

  object findDuplicateRelationships extends (Pattern => Set[Seq[Variable]]) {

    def apply(pattern: Pattern): Set[Seq[Variable]] = {
      val (seen, duplicates) = pattern.fold((Set.empty[Variable], Seq.empty[Variable])) {
        case RelationshipChain(_, RelationshipPattern(Some(rel), _, None, _, _, _), _) =>
          (acc) =>
            val (seen, duplicates) = acc

            val newDuplicates = if (seen.contains(rel)) duplicates :+ rel else duplicates
            val newSeen = seen + rel

            (newSeen, newDuplicates)

        case _ =>
          identity
      }

      val m0: Map[String, Seq[Variable]] = duplicates.groupBy(_.name)

      val resultMap = seen.foldLeft(m0) {
        case (m, ident @ Variable(name)) if m.contains(name) => m.updated(name, Seq(ident) ++ m(name))
        case (m, _)                                            => m
      }

      resultMap.values.toSet
    }
  }
}

case class Pattern(patternParts: Seq[PatternPart])(val position: InputPosition) extends ASTNode {

  lazy val length = this.fold(0) {
    case RelationshipChain(_, _, _) => _ + 1
    case _ => identity
  }
}

case class RelationshipsPattern(element: RelationshipChain)(val position: InputPosition) extends ASTNode


sealed abstract class PatternPart extends ASTNode {
  def element: PatternElement
}

case class NamedPatternPart(variable: Variable, patternPart: AnonymousPatternPart)(val position: InputPosition) extends PatternPart {
  def element: PatternElement = patternPart.element
}


sealed trait AnonymousPatternPart extends PatternPart

case class EveryPath(element: PatternElement) extends AnonymousPatternPart {
  def position = element.position
}

case class ShortestPaths(element: PatternElement, single: Boolean)(val position: InputPosition) extends AnonymousPatternPart {
  val name: String =
    if (single)
      "shortestPath"
    else
      "allShortestPaths"
}

sealed abstract class PatternElement extends ASTNode {
  def allVariables: Set[Variable]
  def variable: Option[Variable]

  def isSingleNode = false
}

case class RelationshipChain(
                              element: PatternElement,
                              relationship: RelationshipPattern,
                              rightNode: NodePattern
                            )(val position: InputPosition)
  extends PatternElement {

  def variable: Option[Variable] = relationship.variable

  override def allVariables: Set[Variable] = element.allVariables ++ relationship.variable ++ rightNode.variable

}

object InvalidNodePattern {
  def apply(id: Variable, labels: Seq[LabelName], properties: Option[Expression])(position: InputPosition) =
    new InvalidNodePattern(id)(position)
}

class InvalidNodePattern(
                          val id: Variable
                        )(
                          position: InputPosition
) extends NodePattern(Some(id), Seq.empty, None)(position) {

  override def canEqual(other: Any): Boolean = other.isInstanceOf[InvalidNodePattern]

  override def equals(other: Any): Boolean = other match {
    case that: InvalidNodePattern =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = 31 * id.hashCode()

  override def allVariables: Set[Variable] = Set.empty
}

case class NodePattern(variable: Option[Variable],
                       labels: Seq[LabelName],
                       properties: Option[Expression])(val position: InputPosition)
  extends PatternElement {

  override def allVariables: Set[Variable] = variable.toSet

  override def isSingleNode = true
}


case class RelationshipPattern(
                                variable: Option[Variable],
                                types: Seq[RelTypeName],
                                length: Option[Option[Range]],
                                properties: Option[Expression],
                                direction: SemanticDirection,
                                legacyTypeSeparator: Boolean = false
                              )(val position: InputPosition) extends ASTNode {

  def isSingleLength: Boolean = length.isEmpty

  def isDirected: Boolean = direction != SemanticDirection.BOTH
}
