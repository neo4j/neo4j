/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CypherType

sealed trait SchemaCommand extends Statement {
  def useGraph: Option[GraphSelection]
  def withGraph(useGraph: Option[GraphSelection]): SchemaCommand

  override def returnColumns: List[LogicalVariable] = List.empty
}

case class CreateIndex(label: LabelName, properties: List[PropertyKeyName], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends SchemaCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()
}

case class CreateIndexNewSyntax(variable: Variable, label: LabelName, properties: List[Property], name: Option[String], ifExistsDo: IfExistsDo, useGraph: Option[GraphSelection] = None)(val position: InputPosition)
  extends SchemaCommand with SemanticAnalysisTooling {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def semanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax() | IfExistsReplace() => SemanticError(s"Failed to create index: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      declareVariable(variable, CTNode) chain
        SemanticExpressionCheck.simple(properties) chain
        semanticCheckFold(properties) {
          property =>
            when(!property.map.isInstanceOf[Variable]) {
              error("Cannot index nested properties", property.position)
            }
        }
  }
}

case class DropIndex(label: LabelName, properties: List[PropertyKeyName], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends SchemaCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  def property = properties.head
  def semanticCheck = Seq()
}

case class DropIndexOnName(name: String, ifExists: Boolean, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends SchemaCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()
}

trait PropertyConstraintCommand extends SchemaCommand with SemanticAnalysisTooling {
  def variable: Variable

  def property: Property

  def entityType: CypherType

  override def semanticCheck =
    declareVariable(variable, entityType) chain
      SemanticExpressionCheck.simple(property) chain
      when(!property.map.isInstanceOf[Variable]) {
        error("Cannot index nested properties", property.position)
      }
}

trait CompositePropertyConstraintCommand extends SchemaCommand with SemanticAnalysisTooling {
  def variable: Variable

  def properties: Seq[Property]

  def entityType: CypherType

  def restrictedToSingleProperty: Boolean

  override def semanticCheck =
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

case class CreateNodeKeyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], name: Option[String], ifExistsDo: IfExistsDo, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodeKeyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax() | IfExistsReplace() => SemanticError(s"Failed to create node key constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ => super.semanticCheck
  }
}

case class DropNodeKeyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodeKeyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateUniquePropertyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], name: Option[String], ifExistsDo: IfExistsDo, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends UniquePropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax() | IfExistsReplace() => SemanticError(s"Failed to create uniqueness constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ => super.semanticCheck
  }
}

case class DropUniquePropertyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends UniquePropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property, name: Option[String], ifExistsDo: IfExistsDo, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodePropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax() | IfExistsReplace() => SemanticError(s"Failed to create node property existence constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ => super.semanticCheck
  }
}

case class DropNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodePropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property, name: Option[String], ifExistsDo: IfExistsDo, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends RelationshipPropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax() | IfExistsReplace() => SemanticError(s"Failed to create relationship property existence constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ => super.semanticCheck
  }
}

case class DropRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends RelationshipPropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class DropConstraintOnName(name: String, ifExists: Boolean, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends SchemaCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()
}
