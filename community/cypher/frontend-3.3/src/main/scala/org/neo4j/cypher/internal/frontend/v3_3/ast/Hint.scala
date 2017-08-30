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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, InternalException, SemanticCheckable}

sealed trait Hint extends ASTNode with ASTPhrase with SemanticCheckable {
  def variables: NonEmptyList[Variable]
}

trait NodeHint {
  self: Hint =>
}

trait RelationshipHint {
  self: Hint =>
}

object Hint {
  implicit val byVariable: Ordering[Hint] =
    Ordering.by { (hint: Hint) => hint.variables.head }(Variable.byName)
}
// allowed on match

sealed trait UsingHint extends Hint

// allowed on start item

sealed trait LegacyIndexHint extends UsingHint {
  self: StartItem =>

  def variable: Variable
  def variables = NonEmptyList(variable)
}

case class UsingIndexHint(variable: Variable, label: LabelName, properties: Seq[PropertyKeyName])(val position: InputPosition) extends UsingHint with NodeHint {
  def variables = NonEmptyList(variable)
  def semanticCheck = variable.ensureVariableDefined chain variable.expectType(CTNode.covariant)

  override def toString: String = s"USING INDEX ${variable.name}:${label.name}(${properties.map(_.name).mkString(", ")})"
}

case class UsingScanHint(variable: Variable, label: LabelName)(val position: InputPosition) extends UsingHint with NodeHint {
  def variables = NonEmptyList(variable)
  def semanticCheck = variable.ensureVariableDefined chain variable.expectType(CTNode.covariant)

  override def toString: String = s"USING SCAN ${variable.name}:${label.name}"
}

object UsingJoinHint {
  import NonEmptyList._

  def apply(elts: Seq[Variable])(pos: InputPosition): UsingJoinHint =
    UsingJoinHint(elts.toNonEmptyListOption.getOrElse(throw new InternalException("Expected non-empty sequence of variables")))(pos)
}

case class UsingJoinHint(variables: NonEmptyList[Variable])(val position: InputPosition) extends UsingHint with NodeHint {
  def semanticCheck =
    variables.map { variable => variable.ensureVariableDefined chain variable.expectType(CTNode.covariant) }.reduceLeft(_ chain _)

  override def toString: String = s"USING JOIN ON ${variables.map(_.name).toIndexedSeq.mkString(", ")}"
}

// start items

sealed trait StartItem extends ASTNode with ASTPhrase with SemanticCheckable {
  def variable: Variable
  def name = variable.name
}

sealed trait NodeStartItem extends StartItem {
  def semanticCheck = variable.declareVariable(CTNode)
}

case class NodeByIdentifiedIndex(variable: Variable, index: String, key: String, value: Expression)(val position: InputPosition)
  extends NodeStartItem with LegacyIndexHint with NodeHint

case class NodeByIndexQuery(variable: Variable, index: String, query: Expression)(val position: InputPosition)
  extends NodeStartItem with LegacyIndexHint with NodeHint

case class NodeByParameter(variable: Variable, parameter: Parameter)(val position: InputPosition) extends NodeStartItem
case class AllNodes(variable: Variable)(val position: InputPosition) extends NodeStartItem

sealed trait RelationshipStartItem extends StartItem {
  def semanticCheck = variable.declareVariable(CTRelationship)
}

case class RelationshipByIds(variable: Variable, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition) extends RelationshipStartItem
case class RelationshipByParameter(variable: Variable, parameter: Parameter)(val position: InputPosition) extends RelationshipStartItem
case class AllRelationships(variable: Variable)(val position: InputPosition) extends RelationshipStartItem
case class RelationshipByIdentifiedIndex(variable: Variable, index: String, key: String, value: Expression)(val position: InputPosition) extends RelationshipStartItem with LegacyIndexHint with RelationshipHint
case class RelationshipByIndexQuery(variable: Variable, index: String, query: Expression)(val position: InputPosition) extends RelationshipStartItem with LegacyIndexHint with RelationshipHint

// no longer supported non-hint legacy start items

case class NodeByIds(variable: Variable, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition) extends NodeStartItem

