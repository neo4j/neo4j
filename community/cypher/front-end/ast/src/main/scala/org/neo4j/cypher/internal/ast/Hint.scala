/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.UnsignedIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.NonEmptyList.IterableConverter
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

sealed trait Hint extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {
  def variables: NonEmptyList[Variable]
}

sealed trait NodeHint extends Hint

object Hint {

  implicit val byVariable: Ordering[Hint] =
    Ordering.by { (hint: Hint) => hint.variables.head }(Variable.byName)
}
// allowed on match

sealed trait UsingHint extends Hint

// allowed on start item

sealed trait UsingIndexHintSpec {
  def fulfilledByScan: Boolean
}

case object SeekOnly extends UsingIndexHintSpec {
  override def fulfilledByScan: Boolean = false
}

case object SeekOrScan extends UsingIndexHintSpec {
  override def fulfilledByScan: Boolean = true
}

sealed trait UsingIndexHintType
case object UsingAnyIndexType extends UsingIndexHintType
case object UsingRangeIndexType extends UsingIndexHintType
case object UsingTextIndexType extends UsingIndexHintType
case object UsingPointIndexType extends UsingIndexHintType

case class UsingIndexHint(
  variable: Variable,
  labelOrRelType: LabelOrRelTypeName,
  properties: Seq[PropertyKeyName],
  spec: UsingIndexHintSpec = SeekOrScan,
  indexType: UsingIndexHintType = UsingAnyIndexType
)(val position: InputPosition) extends UsingHint with NodeHint {
  override def variables: NonEmptyList[Variable] = NonEmptyList(variable)

  override def semanticCheck: SemanticCheck =
    ensureDefined(variable) chain expectType(CTNode.covariant | CTRelationship.covariant, variable)
}

case class UsingScanHint(variable: Variable, labelOrRelType: LabelOrRelTypeName)(val position: InputPosition)
    extends UsingHint with NodeHint {
  override def variables: NonEmptyList[Variable] = NonEmptyList(variable)

  override def semanticCheck: SemanticCheck =
    ensureDefined(variable) chain expectType(CTNode.covariant | CTRelationship.covariant, variable)
}

object UsingJoinHint {

  def apply(elts: Seq[Variable])(pos: InputPosition): UsingJoinHint =
    UsingJoinHint(
      elts.toNonEmptyListOption.getOrElse(throw new IllegalStateException("Expected non-empty sequence of variables"))
    )(pos)
}

case class UsingJoinHint(variables: NonEmptyList[Variable])(val position: InputPosition) extends UsingHint
    with NodeHint {

  override def semanticCheck: SemanticCheck =
    variables.map { variable => ensureDefined(variable) chain expectType(CTNode.covariant, variable) }.reduceLeft(
      _ chain _
    )
}

/**
 * These are never introduced from the parser currently. Thus no need for semantic checks or input positions.
 */
sealed trait UsingStatefulShortestPathHint extends UsingHint {
  override def semanticCheck: SemanticCheck = SemanticCheck.success

  override def position: InputPosition = InputPosition.NONE
}

case class UsingStatefulShortestPathInto(override val variables: NonEmptyList[Variable])
    extends UsingStatefulShortestPathHint

case class UsingStatefulShortestPathAll(override val variables: NonEmptyList[Variable])
    extends UsingStatefulShortestPathHint

// start items

sealed trait StartItem extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {
  def variable: Variable
  def name: String = variable.name
}

sealed trait NodeStartItem extends StartItem {
  def semanticCheck: SemanticCheck = declareVariable(variable, CTNode)
}

case class NodeByParameter(variable: Variable, parameter: Parameter)(val position: InputPosition) extends NodeStartItem
case class AllNodes(variable: Variable)(val position: InputPosition) extends NodeStartItem

sealed trait RelationshipStartItem extends StartItem {
  def semanticCheck: SemanticCheck = declareVariable(variable, CTRelationship)
}

case class RelationshipByIds(variable: Variable, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition)
    extends RelationshipStartItem

case class RelationshipByParameter(variable: Variable, parameter: Parameter)(val position: InputPosition)
    extends RelationshipStartItem
case class AllRelationships(variable: Variable)(val position: InputPosition) extends RelationshipStartItem

// no longer supported non-hint legacy start items

case class NodeByIds(variable: Variable, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition)
    extends NodeStartItem
