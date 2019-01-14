/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticAnalysisTooling, SemanticError, SemanticExpressionCheck}
import org.neo4j.cypher.internal.util.v3_4.symbols.{CypherType, _}
import org.neo4j.cypher.internal.v3_4.expressions._

sealed trait Command extends Statement {
  override def returnColumns = List.empty
}

case class CreateIndex(label: LabelName, properties: List[PropertyKeyName])(val position: InputPosition) extends Command {
//  def property = properties(0)
  def semanticCheck = Seq()
}

case class DropIndex(label: LabelName, properties: List[PropertyKeyName])(val position: InputPosition) extends Command {
  def property = properties.head
  def semanticCheck = Seq()
}

trait PropertyConstraintCommand extends Command with SemanticAnalysisTooling {
  def variable: Variable

  def property: Property

  def entityType: CypherType

  def semanticCheck =
    declareVariable(variable, entityType) chain
      SemanticExpressionCheck.simple(property) chain
      when(!property.map.isInstanceOf[Variable]) {
        error("Cannot index nested properties", property.position)
      }
}

trait CompositePropertyConstraintCommand extends Command with SemanticAnalysisTooling {
  def variable: Variable

  def properties: Seq[Property]

  def entityType: CypherType

  def restrictedToSingleProperty: Boolean

  def semanticCheck =
    declareVariable(variable, entityType) chain
      SemanticExpressionCheck.simple(properties) chain
      semanticCheckFold(properties) {
        property =>
          when(!property.map.isInstanceOf[Variable]) {
            error("Cannot index nested properties", property.position)
          }
      } chain
      when(restrictedToSingleProperty && properties.size > 1) {
        error("Only single property uniqueness constraints are supported", properties(1).position)
      }
}

trait NodePropertyConstraintCommand extends PropertyConstraintCommand {

  val entityType = CTNode

  def label: LabelName
}

trait UniquePropertyConstraintCommand extends CompositePropertyConstraintCommand {

  val entityType = CTNode

  def label: LabelName

  override def restrictedToSingleProperty: Boolean = true
}

trait NodeKeyConstraintCommand extends CompositePropertyConstraintCommand {

  val entityType = CTNode

  def label: LabelName

  override def restrictedToSingleProperty: Boolean = false
}

trait RelationshipPropertyConstraintCommand extends PropertyConstraintCommand {

  val entityType = CTRelationship

  def relType: RelTypeName
}

case class CreateNodeKeyConstraint(variable: Variable, label: LabelName, properties: Seq[Property])(val position: InputPosition) extends NodeKeyConstraintCommand

case class DropNodeKeyConstraint(variable: Variable, label: LabelName, properties: Seq[Property])(val position: InputPosition) extends NodeKeyConstraintCommand

case class CreateUniquePropertyConstraint(variable: Variable, label: LabelName, properties: Seq[Property])(val position: InputPosition) extends UniquePropertyConstraintCommand

case class DropUniquePropertyConstraint(variable: Variable, label: LabelName, properties: Seq[Property])(val position: InputPosition) extends UniquePropertyConstraintCommand

case class CreateNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property)(val position: InputPosition) extends NodePropertyConstraintCommand

case class DropNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property)(val position: InputPosition) extends NodePropertyConstraintCommand

case class CreateRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property)(val position: InputPosition) extends RelationshipPropertyConstraintCommand

case class DropRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property)(val position: InputPosition) extends RelationshipPropertyConstraintCommand
