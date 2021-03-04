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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.expressions.Expression
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

sealed trait ReadSchemaCommand extends SchemaCommand {
  override def containsUpdates: Boolean = false
}
// TODO: once show constraints are moved as well, ReadSchemaCommand can die and WriteSchemaCommand can be merged with SchemaCommand
sealed trait WriteSchemaCommand extends SchemaCommand {
  override def containsUpdates: Boolean = true
}

case class CreateIndexOldSyntax(label: LabelName, properties: List[PropertyKeyName], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends WriteSchemaCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()
}

abstract class CreateIndex(variable: Variable, properties: List[Property], ifExistsDo: IfExistsDo, options: Map[String, Expression])(val position: InputPosition)
  extends WriteSchemaCommand with SemanticAnalysisTooling {
  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => SemanticError(s"Failed to create index: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      // The validation of the values (provider, config keys and config values) are done at runtime.
      if (options.filterKeys(k => !k.equalsIgnoreCase("indexProvider") && !k.equalsIgnoreCase("indexConfig")).nonEmpty)
        SemanticError(s"Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.", position)
      else
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

case class CreateNodeIndex(variable: Variable, label: LabelName, properties: List[Property], name: Option[String], ifExistsDo: IfExistsDo, options: Map[String, Expression], useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, options)(position) {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateRelationshipIndex(variable: Variable, relType: RelTypeName, properties: List[Property], name: Option[String], ifExistsDo: IfExistsDo, options: Map[String, Expression], useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, options)(position) {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class DropIndex(label: LabelName, properties: List[PropertyKeyName], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends WriteSchemaCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  def property: PropertyKeyName = properties.head
  def semanticCheck = Seq()
}

case class DropIndexOnName(name: String, ifExists: Boolean, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends WriteSchemaCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()
}

trait PropertyConstraintCommand extends WriteSchemaCommand with SemanticAnalysisTooling {
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

trait CompositePropertyConstraintCommand extends WriteSchemaCommand with SemanticAnalysisTooling {
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

case class CreateNodeKeyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], name: Option[String], ifExistsDo: IfExistsDo, options: Map[String, Expression], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodeKeyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => SemanticError(s"Failed to create node key constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      // The validation of the values (provider, config keys and config values) are done at runtime.
      if (options.filterKeys(k => !k.equalsIgnoreCase("indexProvider") && !k.equalsIgnoreCase("indexConfig")).nonEmpty)
        SemanticError(s"Failed to create node key constraint: Invalid option provided, valid options are `indexProvider` and `indexConfig`.", position)
      else
        super.semanticCheck
  }
}

case class DropNodeKeyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodeKeyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateUniquePropertyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], name: Option[String], ifExistsDo: IfExistsDo, options: Map[String, Expression], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends UniquePropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => SemanticError(s"Failed to create uniqueness constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      // The validation of the values (provider, config keys and config values) are done at runtime.
      if (options.filterKeys(k => !k.equalsIgnoreCase("indexProvider") && !k.equalsIgnoreCase("indexConfig")).nonEmpty)
        SemanticError(s"Failed to create uniqueness constraint: Invalid option provided, valid options are `indexProvider` and `indexConfig`.", position)
      else
        super.semanticCheck
  }
}

case class DropUniquePropertyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends UniquePropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property, name: Option[String], ifExistsDo: IfExistsDo, oldSyntax: Boolean, options: Option[Map[String, Expression]] = None, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodePropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => SemanticError(s"Failed to create node property existence constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      if (options.isDefined) SemanticError(s"Failed to create node property existence constraint: `OPTIONS` cannot be used together with this command.", position)
      else super.semanticCheck
  }
}

case class DropNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodePropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property, name: Option[String], ifExistsDo: IfExistsDo, oldSyntax: Boolean, options: Option[Map[String, Expression]] = None, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends RelationshipPropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => SemanticError(s"Failed to create relationship property existence constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      if (options.isDefined) SemanticError(s"Failed to create relationship property existence constraint: `OPTIONS` cannot be used together with this command.", position)
      else super.semanticCheck
  }
}

case class DropRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends RelationshipPropertyConstraintCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class DropConstraintOnName(name: String, ifExists: Boolean, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends WriteSchemaCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()
}

case class ShowConstraints(constraintType: ShowConstraintType, verbose: Boolean, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends ReadSchemaCommand {
  override def withGraph(useGraph: Option[GraphSelection]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()

  private val briefColumnNames: List[String] = List("id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndexId")
  val defaultColumnNames: List[String] = if (verbose) briefColumnNames ++ List("options", "createStatement") else briefColumnNames

  override def returnColumns: List[LogicalVariable] = defaultColumnNames.map(name => Variable(name)(position))
}

sealed trait ShowConstraintType {
  val output: String
  val prettyPrint: String
}

case object AllConstraints extends ShowConstraintType {
  override val output: String = "ALL"
  override val prettyPrint: String = "ALL"
}

case object UniqueConstraints extends ShowConstraintType {
  override val output: String = "UNIQUENESS"
  override val prettyPrint: String = "UNIQUE"
}

case class ExistsConstraints(syntax: ExistenceConstraintSyntax) extends ShowConstraintType {
  override val output: String = "PROPERTY_EXISTENCE"
  override val prettyPrint: String = syntax.keyword
}

case class NodeExistsConstraints(syntax: ExistenceConstraintSyntax = NewSyntax) extends ShowConstraintType {
  override val output: String = "NODE_PROPERTY_EXISTENCE"
  override val prettyPrint: String = s"NODE ${syntax.keyword}"
}

case class RelExistsConstraints(syntax: ExistenceConstraintSyntax = NewSyntax) extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_PROPERTY_EXISTENCE"
  override val prettyPrint: String = s"RELATIONSHIP ${syntax.keyword}"
}

case object NodeKeyConstraints extends ShowConstraintType {
  override val output: String = "NODE_KEY"
  override val prettyPrint: String = "NODE KEY"
}

sealed trait ExistenceConstraintSyntax {
  val keyword: String
}

case object DeprecatedSyntax extends ExistenceConstraintSyntax {
  override val keyword: String = "EXISTS"
}

case object OldValidSyntax extends ExistenceConstraintSyntax {
  override val keyword: String = "EXIST"
}

case object NewSyntax extends ExistenceConstraintSyntax {
  override val keyword: String = "EXISTENCE"
}
