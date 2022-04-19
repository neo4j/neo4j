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
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.RelationshipType

sealed trait SchemaCommand extends StatementWithGraph with SemanticAnalysisTooling {
  def useGraph: Option[GraphSelection]
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand

  override def returnColumns: List[LogicalVariable] = List.empty

  override def containsUpdates: Boolean = true

  // The validation of the values (provider, config keys and config values) are done at runtime.
  protected def checkOptionsMap(schemaString: String, options: Options): SemanticCheck = options match {
    case OptionsMap(ops) if ops.filterKeys(k => !k.equalsIgnoreCase("indexProvider") && !k.equalsIgnoreCase("indexConfig")).nonEmpty =>
      error(s"Failed to create $schemaString: Invalid option provided, valid options are `indexProvider` and `indexConfig`.", position)
    case _ => SemanticCheckResult.success
  }

  protected def checkSingleProperty(schemaString: String, properties: List[Property]): SemanticCheck = when(properties.size > 1) {
    error(s"Only single property $schemaString are supported", properties(1).position)
  }

  // Error messages for mixing old and new constraint syntax
  protected val errorMessageOnRequire: String = "Invalid constraint syntax, ON should not be used in combination with REQUIRE. Replace ON with FOR."
  protected val errorMessageForAssert: String = "Invalid constraint syntax, FOR should not be used in combination with ASSERT. Replace ASSERT with REQUIRE."
  protected val errorMessageForAssertExists: String = "Invalid constraint syntax, FOR should not be used in combination with ASSERT EXISTS. Replace ASSERT EXISTS with REQUIRE ... IS NOT NULL."
}

case class CreateIndexOldSyntax(label: LabelName, properties: List[PropertyKeyName], useGraph: Option[UseGraph] = None)(val position: InputPosition) extends SchemaCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()
}

abstract class CreateIndex(variable: Variable, properties: List[Property], ifExistsDo: IfExistsDo, isNodeIndex: Boolean)(val position: InputPosition)
  extends SchemaCommand {
  // To anonymize the name
  val name: Option[String]
  def withName(name: Option[String]): CreateIndex

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => error("Failed to create index: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      val ctType = if (isNodeIndex) CTNode else CTRelationship
      declareVariable(variable, ctType) chain
        SemanticExpressionCheck.simple(properties) chain
        semanticCheckFold(properties) {
          property =>
            when(!property.map.isInstanceOf[Variable]) {
              error("Cannot index nested properties", property.position)
            }
        }
  }
}

case class CreateBtreeNodeIndex(variable: Variable, label: LabelName, properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateBtreeNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck = checkOptionsMap("btree node index", options) chain super.semanticCheck
}

case class CreateBtreeRelationshipIndex(variable: Variable, relType: RelTypeName, properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateBtreeRelationshipIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck = checkOptionsMap("btree relationship property index", options) chain super.semanticCheck
}

case class CreateRangeNodeIndex(variable: Variable, label: LabelName, properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, fromDefault: Boolean, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateRangeNodeIndex = copy(name = name)(position)

  // We want to make RANGE default eventually, but for now we keep the behavior of BTREE being default
  override def semanticCheck: SemanticCheck = checkOptionsMap(if (fromDefault) "btree node index" else "range node property index", options) chain super.semanticCheck
}

case class CreateRangeRelationshipIndex(variable: Variable, relType: RelTypeName, properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, fromDefault: Boolean, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateRangeRelationshipIndex = copy(name = name)(position)

  // We want to make RANGE default eventually, but for now we keep the behavior of BTREE being default
  override def semanticCheck: SemanticCheck = checkOptionsMap(if (fromDefault) "btree relationship property index" else "range relationship property index", options) chain super.semanticCheck
}

case class CreateLookupIndex(variable: Variable, isNodeIndex: Boolean, function: FunctionInvocation, override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, List.empty, ifExistsDo, isNodeIndex)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateLookupIndex = copy(name = name)(position)

  private def allowedFunction(name: String): Boolean = if (isNodeIndex) name.equalsIgnoreCase(Labels.name) else name.equalsIgnoreCase(Type.name)

  override def semanticCheck: SemanticCheck = function match {
    case FunctionInvocation(_, FunctionName(name), _, _) if !allowedFunction(name) =>
      if (isNodeIndex) error(s"Failed to create node lookup index: Function '$name' is not allowed, valid function is '${Labels.name}'.", position)
      else error(s"Failed to create relationship lookup index: Function '$name' is not allowed, valid function is '${Type.name}'.", position)
    case _ =>
      checkOptionsMap("token lookup index", options) chain super.semanticCheck chain SemanticExpressionCheck.simple(function)
  }
}

case class CreateFulltextNodeIndex(variable: Variable, label: List[LabelName], properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateFulltextNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck = checkOptionsMap("fulltext node index", options) chain super.semanticCheck
}

case class CreateFulltextRelationshipIndex(variable: Variable, relType: List[RelTypeName], properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateFulltextRelationshipIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck = checkOptionsMap("fulltext relationship index", options) chain super.semanticCheck
}

case class CreateTextNodeIndex(variable: Variable, label: LabelName, properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateTextNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("text node index", options) chain
      super.semanticCheck chain
      checkSingleProperty("text indexes", properties)
}

case class CreateTextRelationshipIndex(variable: Variable, relType: RelTypeName, properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateTextRelationshipIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("text relationship index", options) chain
      super.semanticCheck chain
      checkSingleProperty("text indexes", properties)
}

case class CreatePointNodeIndex(variable: Variable, label: LabelName, properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreatePointNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("point node index", options) chain
      super.semanticCheck chain
      checkSingleProperty("point indexes", properties)
}

case class CreatePointRelationshipIndex(variable: Variable, relType: RelTypeName, properties: List[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, useGraph: Option[GraphSelection] = None)(override val position: InputPosition)
  extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreatePointRelationshipIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("point relationship index", options) chain
      super.semanticCheck chain
      checkSingleProperty("point indexes", properties)
}

case class DropIndex(label: LabelName, properties: List[PropertyKeyName], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends SchemaCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  def property: PropertyKeyName = properties.head
  def semanticCheck = Seq()
}

case class DropIndexOnName(name: String, ifExists: Boolean, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends SchemaCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()
}

trait PropertyConstraintCommand extends SchemaCommand {
  def variable: Variable

  def property: Property

  def entityType: CypherType

  override def semanticCheck: SemanticCheck =
    declareVariable(variable, entityType) chain
      SemanticExpressionCheck.simple(property) chain
      when(!property.map.isInstanceOf[Variable]) {
        error("Cannot index nested properties", property.position)
      }
}

trait CompositePropertyConstraintCommand extends SchemaCommand {
  def variable: Variable

  def properties: Seq[Property]

  def entityType: CypherType

  override def semanticCheck: SemanticCheck =
    declareVariable(variable, entityType) chain
      SemanticExpressionCheck.simple(properties) chain
      semanticCheckFold(properties) {
        property =>
          when(!property.map.isInstanceOf[Variable]) {
            error("Cannot index nested properties", property.position)
          }
      }
}

trait NodePropertyConstraintCommand extends PropertyConstraintCommand {

  val entityType:NodeType = CTNode

  def label: LabelName
}

trait UniquePropertyConstraintCommand extends CompositePropertyConstraintCommand {

  val entityType:NodeType = CTNode

  def label: LabelName
}

trait NodeKeyConstraintCommand extends CompositePropertyConstraintCommand {

  val entityType:NodeType = CTNode

  def label: LabelName
}

trait RelationshipPropertyConstraintCommand extends PropertyConstraintCommand {

  val entityType:RelationshipType = CTRelationship

  def relType: RelTypeName
}

trait CreateConstraint extends SchemaCommand {
  // To anonymize the name
  val name: Option[String]
  def withName(name: Option[String]): CreateConstraint
}

case class CreateNodeKeyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, containsOn: Boolean, constraintVersion: ConstraintVersion, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodeKeyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateNodeKeyConstraint = copy(name = name)(position)

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => error(s"Failed to create node key constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
        constraintVersion match {
          case ConstraintVersion2 if containsOn => error(errorMessageOnRequire, position)
          case ConstraintVersion0 if !containsOn => error(errorMessageForAssert, position)
          case _ => checkOptionsMap("node key constraint", options) chain super.semanticCheck
        }
  }
}

case class DropNodeKeyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodeKeyConstraintCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateUniquePropertyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, containsOn: Boolean, constraintVersion: ConstraintVersion, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends UniquePropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateUniquePropertyConstraint = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => error(s"Failed to create uniqueness constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      constraintVersion match {
        case ConstraintVersion2 if containsOn => error(errorMessageOnRequire, position)
        case ConstraintVersion0 if !containsOn => error(errorMessageForAssert, position)
        case _ => checkOptionsMap("uniqueness constraint", options) chain super.semanticCheck
      }
  }
}

case class DropUniquePropertyConstraint(variable: Variable, label: LabelName, properties: Seq[Property], useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends UniquePropertyConstraintCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property, override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, containsOn: Boolean, constraintVersion: ConstraintVersion, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodePropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateNodePropertyExistenceConstraint = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => error(s"Failed to create node property existence constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      constraintVersion match {
        case ConstraintVersion2 if containsOn => error(errorMessageOnRequire, position)
        case ConstraintVersion1 if !containsOn => error(errorMessageForAssert, position)
        case ConstraintVersion0 if !containsOn => error(errorMessageForAssertExists, position)
        case _ => checkOptionsMap("node property existence constraint", options) chain super.semanticCheck
      }
  }
}

case class DropNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends NodePropertyConstraintCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class CreateRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property, override val name: Option[String], ifExistsDo: IfExistsDo, options: Options, containsOn: Boolean, constraintVersion: ConstraintVersion, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends RelationshipPropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[String]): CreateRelationshipPropertyExistenceConstraint = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =  ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace => error(s"Failed to create relationship property existence constraint: `OR REPLACE` cannot be used together with this command.", position)
    case _ =>
      constraintVersion match {
        case ConstraintVersion2 if containsOn => error(errorMessageOnRequire, position)
        case ConstraintVersion1 if !containsOn => error(errorMessageForAssert, position)
        case ConstraintVersion0 if !containsOn => error(errorMessageForAssertExists, position)
        case _ => checkOptionsMap("relationship property existence constraint", options) chain super.semanticCheck
      }
  }
}

case class DropRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends RelationshipPropertyConstraintCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
}

case class DropConstraintOnName(name: String, ifExists: Boolean, useGraph: Option[GraphSelection] = None)(val position: InputPosition) extends SchemaCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  def semanticCheck = Seq()
}
