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

import org.neo4j.cypher.internal.ast.AdministrationCommand.checkIsStringLiteralOrParameter
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType.AlterOperation
import org.neo4j.cypher.internal.ast.semantics.CypherTypeChecking
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics._
import org.neo4j.cypher.internal.expressions.DynamicLabelExpression
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

sealed trait SchemaCommand extends StatementWithGraph with SemanticAnalysisTooling {

  def commandDescription: String

  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand

  override def returnColumns: List[LogicalVariable] = List.empty

  override def containsUpdates: Boolean = true

  protected def checkSingleProperty(schemaString: String, properties: List[Property]): SemanticCheck = {
    when(properties.size > 1) {
      val position = properties(1).position
      val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .withCause(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N16)
            .withParam(GqlParams.StringParam.idxType, schemaString)
            .atPosition(position.offset, position.line, position.column)
            .build()
        )
        .atPosition(position.offset, position.line, position.column)
        .build()
      error(gql, s"Only single property $schemaString are supported", properties(1).position)
    }
  }
}

// Indexes

sealed trait CreateIndex extends SchemaCommand {
  override def commandDescription: String = "CREATE " + indexType.command

  // To anonymize the name
  val name: Option[Expression]
  def withName(name: Option[Expression]): CreateIndex

  def indexType: CreateIndexType
  def variable: Variable
  def isNodeIndex: Boolean
  def properties: List[Property]
  def ifExistsDo: IfExistsDo
  def options: Options

  // Vector indexes have two lists of properties to be checked, so it overrides this to add both it's lists
  protected def propertiesForSemanticCheck: List[Property] = properties

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace =>
      SemanticCheck.error(SemanticError.badCommandWithOrReplace("create index", "CREATE INDEX", position))
    case _ =>
      val ctType = if (isNodeIndex) CTNode else CTRelationship
      name.map(checkIsStringLiteralOrParameter("index name", _)).getOrElse(SemanticCheck.success) chain
        declareVariable(variable, ctType) chain
        SemanticExpressionCheck.simple(propertiesForSemanticCheck) chain
        semanticCheckFold(propertiesForSemanticCheck) {
          property =>
            when(!property.map.isInstanceOf[Variable]) {
              // This is unreachable, the parser only produces variables for Property for CreateIndex/Constraint
              val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
                .atPosition(property.position.offset, property.position.line, property.position.column)
                .withParam(GqlParams.StringParam.msgTitle, "Syntax Exception")
                .withParam(GqlParams.StringParam.msg, "Cannot index nested properties.")
                .build()
              error(gql, "Cannot index nested properties", property.position)
            }
        } chain {
          if (indexType.singlePropertyOnly) checkSingleProperty(indexType.allDescription, propertiesForSemanticCheck)
          else SemanticCheck.success
        }
  }
}

object CreateIndex {

  // Help methods for creating the different index types

  def createFulltextNodeIndex(
    variable: Variable,
    labels: List[LabelName],
    properties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateFulltextIndexCommand(
      variable,
      entityNames = Left(labels),
      properties,
      name,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createFulltextRelationshipIndex(
    variable: Variable,
    relTypes: List[RelTypeName],
    properties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateFulltextIndexCommand(
      variable,
      entityNames = Right(relTypes),
      properties,
      name,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createLookupIndex(
    variable: Variable,
    isNodeIndex: Boolean,
    function: FunctionInvocation,
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateLookupIndexCommand(
      variable,
      isNodeIndex,
      function,
      name,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createPointNodeIndex(
    variable: Variable,
    label: LabelName,
    properties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = label,
      properties,
      name,
      indexType = PointCreateIndex,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createPointRelationshipIndex(
    variable: Variable,
    relType: RelTypeName,
    properties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = relType,
      properties,
      name,
      indexType = PointCreateIndex,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createRangeNodeIndex(
    variable: Variable,
    label: LabelName,
    properties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    fromDefault: Boolean,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = label,
      properties,
      name,
      indexType = RangeCreateIndex(fromDefault),
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createRangeRelationshipIndex(
    variable: Variable,
    relType: RelTypeName,
    properties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    fromDefault: Boolean,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = relType,
      properties,
      name,
      indexType = RangeCreateIndex(fromDefault),
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createTextNodeIndex(
    variable: Variable,
    label: LabelName,
    properties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = label,
      properties,
      name,
      indexType = TextCreateIndex,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createTextRelationshipIndex(
    variable: Variable,
    relType: RelTypeName,
    properties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = relType,
      properties,
      name,
      indexType = TextCreateIndex,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createVectorNodeIndex(
    variable: Variable,
    labels: List[LabelName],
    properties: List[Property],
    additionalProperties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateVectorIndexCommand(
      variable,
      entityNames = Left(labels),
      properties,
      additionalProperties,
      name,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createVectorRelationshipIndex(
    variable: Variable,
    relTypes: List[RelTypeName],
    properties: List[Property],
    additionalProperties: List[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateVectorIndexCommand(
      variable,
      entityNames = Right(relTypes),
      properties,
      additionalProperties,
      name,
      ifExistsDo,
      options,
      useGraph
    )(position)

}

sealed trait CreateSingleLabelPropertyIndex extends CreateIndex {
  def entityName: ElementTypeName

  val (isNodeIndex: Boolean, entityIndexDescription: String) = entityName match {
    case _: LabelName   => (true, indexType.nodeDescription)
    case _: RelTypeName => (false, indexType.relDescription)
    case _: DynamicLabelExpression =>
      throw new IllegalStateException(
        s"Did not expect Dynamic Labels here"
      )
    case _: DynamicRelTypeExpression =>
      throw new IllegalStateException(
        s"Did not expect Dynamic Relationships here"
      )
  }

  override def semanticCheck: SemanticCheck =
    options.checkOptionsForSchema(entityIndexDescription) chain super.semanticCheck
}

object CreateSingleLabelPropertyIndex {

  def unapply(c: CreateSingleLabelPropertyIndex): Some[(
    Variable,
    ElementTypeName,
    List[Property],
    Option[Expression],
    CreateIndexType,
    IfExistsDo,
    Options
  )] =
    Some((c.variable, c.entityName, c.properties, c.name, c.indexType, c.ifExistsDo, c.options))
}

sealed trait CreateFulltextIndex extends CreateIndex {
  def entityNames: Either[List[LabelName], List[RelTypeName]]

  override val indexType: CreateIndexType = FulltextCreateIndex

  val (isNodeIndex: Boolean, entityIndexDescription: String) = entityNames match {
    case Left(_)  => (true, indexType.nodeDescription)
    case Right(_) => (false, indexType.relDescription)
  }

  override def semanticCheck: SemanticCheck =
    options.checkOptionsForSchema(entityIndexDescription) chain super.semanticCheck
}

object CreateFulltextIndex {

  def unapply(c: CreateFulltextIndex): Some[(
    Variable,
    Either[List[LabelName], List[RelTypeName]],
    List[Property],
    Option[Expression],
    CreateIndexType,
    IfExistsDo,
    Options
  )] =
    Some((c.variable, c.entityNames, c.properties, c.name, c.indexType, c.ifExistsDo, c.options))
}

sealed trait CreateVectorIndex extends CreateIndex {
  def entityNames: Either[List[LabelName], List[RelTypeName]]
  def additionalProperties: List[Property]
  override def propertiesForSemanticCheck: List[Property] = properties ++ additionalProperties

  override val indexType: CreateIndexType = VectorCreateIndex

  val (isNodeIndex: Boolean, entityIndexDescription: String) = entityNames match {
    case Left(_)  => (true, indexType.nodeDescription)
    case Right(_) => (false, indexType.relDescription)
  }

  override def semanticCheck: SemanticCheck =
    options.checkOptionsForSchema(entityIndexDescription) chain
      // While vector indexes allow multiple additional properties, they only allow a single vector property
      checkSingleProperty(indexType.allDescription, properties) chain
      super.semanticCheck
}

object CreateVectorIndex {

  def unapply(c: CreateVectorIndex): Some[(
    Variable,
    Either[List[LabelName], List[RelTypeName]],
    List[Property],
    List[Property],
    Option[Expression],
    CreateIndexType,
    IfExistsDo,
    Options
  )] =
    Some((
      c.variable,
      c.entityNames,
      c.properties,
      c.additionalProperties,
      c.name,
      c.indexType,
      c.ifExistsDo,
      c.options
    ))
}

sealed trait CreateLookupIndex extends CreateIndex {
  def function: FunctionInvocation

  override val indexType: CreateIndexType = LookupCreateIndex
  override val properties: List[Property] = List.empty

  private def allowedFunction(name: String): Boolean =
    if (isNodeIndex) name.equalsIgnoreCase(Labels.name) else name.equalsIgnoreCase(Type.name)

  override def semanticCheck: SemanticCheck = function match {
    case fi: FunctionInvocation if !(fi.isBuiltIn && allowedFunction(fi.name)) =>
      val (validFunction, entityIndexDescription) =
        if (isNodeIndex) (Labels.name, indexType.nodeDescription)
        else (Type.name, indexType.relDescription)
      SemanticCheck.error(SemanticError.invalidFunctionForIndex(
        entityIndexDescription,
        fi.name,
        validFunction,
        position
      ))
    case _ =>
      options.checkOptionsForSchema(indexType.allDescription) chain
        super.semanticCheck chain
        SemanticExpressionCheck.simple(function)
  }
}

object CreateLookupIndex {

  def unapply(c: CreateLookupIndex): Some[(
    Variable,
    Boolean,
    FunctionInvocation,
    Option[Expression],
    CreateIndexType,
    IfExistsDo,
    Options
  )] =
    Some((c.variable, c.isNodeIndex, c.function, c.name, c.indexType, c.ifExistsDo, c.options))
}

private case class CreateSingleLabelPropertyIndexCommand(
  variable: Variable,
  entityName: ElementTypeName,
  properties: List[Property],
  override val name: Option[Expression],
  indexType: CreateIndexType,
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateSingleLabelPropertyIndex {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Expression]): CreateSingleLabelPropertyIndexCommand =
    copy(name = name)(position)
}

private case class CreateFulltextIndexCommand(
  variable: Variable,
  entityNames: Either[List[LabelName], List[RelTypeName]],
  properties: List[Property],
  override val name: Option[Expression],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateFulltextIndex {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Expression]): CreateFulltextIndexCommand =
    copy(name = name)(position)
}

private case class CreateVectorIndexCommand(
  variable: Variable,
  entityNames: Either[List[LabelName], List[RelTypeName]],
  properties: List[Property],
  additionalProperties: List[Property],
  override val name: Option[Expression],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateVectorIndex {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Expression]): CreateVectorIndexCommand =
    copy(name = name)(position)
}

private case class CreateLookupIndexCommand(
  variable: Variable,
  isNodeIndex: Boolean,
  function: FunctionInvocation,
  override val name: Option[Expression],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateLookupIndex {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Expression]): CreateLookupIndexCommand = copy(name = name)(position)
}

case class DropIndexOnName(
  name: Expression,
  ifExists: Boolean,
  useGraph: Option[GraphSelection] = None
)(
  val position: InputPosition
) extends SchemaCommand {
  override val commandDescription: String = "DROP INDEX"
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def semanticCheck: SemanticCheck = checkIsStringLiteralOrParameter("index name", name)
}

// Constraints

sealed trait CreateConstraint extends SchemaCommand {
  override def commandDescription: String = "CREATE CONSTRAINT ... " + constraintType.predicate

  // To anonymize the name
  val name: Option[Expression]
  def withName(name: Option[Expression]): CreateConstraint

  def constraintType: CreateConstraintType
  def variable: Variable
  def entityType: CypherType
  def entityName: ElementTypeName
  def properties: Seq[Property]
  def ifExistsDo: IfExistsDo
  def options: Options

  override def semanticCheck: SemanticCheck = {
    name.map(checkIsStringLiteralOrParameter("constraint name", _)).getOrElse(SemanticCheck.success) chain
      declareVariable(variable, entityType) chain
      SemanticExpressionCheck.simple(properties) chain
      semanticCheckFold(properties) {
        property =>
          when(!property.map.isInstanceOf[Variable]) {
            // This is unreachable, the parser only produces variables for Property for CreateIndex/Constraint
            val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
              .atPosition(property.position.offset, property.position.line, property.position.column)
              .withParam(GqlParams.StringParam.msgTitle, "Syntax Exception")
              .withParam(GqlParams.StringParam.msg, "Cannot index nested properties.")
              .build()
            error(gql, "Cannot index nested properties", property.position)
          }
      }
  }

  protected def checkIfExistsDoAndOptions(): SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace =>
      SemanticCheck.error(SemanticError.badCommandWithOrReplace(
        s"create ${constraintType.description} constraint",
        "CREATE CONSTRAINT",
        position
      ))
    case _ =>
      options.checkOptionsForSchema(s"${constraintType.description} constraint")
  }

  protected def checkPropertyTypes(
    originalPropertyType: CypherType,
    normalizedPropertyType: CypherType
  ): SemanticCheck = CypherTypeChecking.checkPropertyTypeForConstraint(
    originalPropertyType,
    normalizedPropertyType,
    SemanticError.propertyTypeUnsupportedInConstraint(constraintType.description, _, _, _)
  )
}

object CreateConstraint {

  def unapply(c: CreateConstraint): Some[(
    Variable,
    ElementTypeName,
    Seq[Property],
    Option[Expression],
    CreateConstraintType,
    IfExistsDo,
    Options
  )] =
    Some((c.variable, c.entityName, c.properties, c.name, c.constraintType, c.ifExistsDo, c.options))

  // Help methods for creating the different constraint types

  def createNodeKeyConstraint(
    variable: Variable,
    label: LabelName,
    properties: Seq[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    fromCypher5: Boolean,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = label,
      properties,
      name,
      constraintType = if (fromCypher5) NodeKey.cypher5 else NodeKey.cypher25,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createRelationshipKeyConstraint(
    variable: Variable,
    relType: RelTypeName,
    properties: Seq[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    fromCypher5: Boolean,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = relType,
      properties,
      name,
      constraintType = if (fromCypher5) RelationshipKey.cypher5 else RelationshipKey.cypher25,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createNodePropertyUniquenessConstraint(
    variable: Variable,
    label: LabelName,
    properties: Seq[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    fromCypher5: Boolean,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = label,
      properties,
      name,
      constraintType = if (fromCypher5) NodePropertyUniqueness.cypher5 else NodePropertyUniqueness.cypher25,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createRelationshipPropertyUniquenessConstraint(
    variable: Variable,
    relType: RelTypeName,
    properties: Seq[Property],
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    fromCypher5: Boolean,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = relType,
      properties,
      name,
      constraintType =
        if (fromCypher5) RelationshipPropertyUniqueness.cypher5 else RelationshipPropertyUniqueness.cypher25,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createNodePropertyExistenceConstraint(
    variable: Variable,
    label: LabelName,
    property: Property,
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = label,
      Seq(property),
      name,
      constraintType = NodePropertyExistence,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createRelationshipPropertyExistenceConstraint(
    variable: Variable,
    relType: RelTypeName,
    property: Property,
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = relType,
      Seq(property),
      name,
      constraintType = RelationshipPropertyExistence,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createNodePropertyTypeConstraint(
    variable: Variable,
    label: LabelName,
    property: Property,
    propertyType: CypherType,
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreatePropertyTypeConstraint(
      variable,
      entityName = label,
      property,
      propertyType,
      name,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createRelationshipPropertyTypeConstraint(
    variable: Variable,
    relType: RelTypeName,
    property: Property,
    propertyType: CypherType,
    name: Option[Expression],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreatePropertyTypeConstraint(
      variable,
      entityName = relType,
      property,
      propertyType,
      name,
      ifExistsDo,
      options,
      useGraph
    )(position)
}

private case class CreateConstraintCommand(
  variable: Variable,
  entityName: ElementTypeName,
  properties: Seq[Property],
  override val name: Option[Expression],
  constraintType: CreateConstraintType,
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Expression]): CreateConstraintCommand = copy(name = name)(position)

  val entityType: CypherType = entityName match {
    case _: LabelName   => CTNode
    case _: RelTypeName => CTRelationship
    case _: DynamicLabelExpression =>
      throw new IllegalStateException(
        s"Did not expect Dynamic Labels here"
      )
    case _: DynamicRelTypeExpression =>
      throw new IllegalStateException(
        s"Did not expect Dynamic Relationships here"
      )
  }

  override def semanticCheck: SemanticCheck = checkIfExistsDoAndOptions() chain super.semanticCheck
}

private case class CreatePropertyTypeConstraint(
  variable: Variable,
  entityName: ElementTypeName,
  property: Property,
  private val propertyType: CypherType,
  override val name: Option[Expression],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Expression]): CreatePropertyTypeConstraint =
    copy(name = name)(position)

  val properties: Seq[Property] = Seq(property)

  // Accessed through the constraint type instead of directly
  private val normalizedPropertyType: CypherType = CypherType.normalizeTypes(propertyType)

  val (entityType: CypherType, constraintType: CreateConstraintType) = entityName match {
    case _: LabelName                => (CTNode, NodePropertyType(normalizedPropertyType))
    case _: RelTypeName              => (CTRelationship, RelationshipPropertyType(normalizedPropertyType))
    case _: DynamicLabelExpression   => (CTNode, NodePropertyType(normalizedPropertyType))
    case _: DynamicRelTypeExpression => (CTRelationship, RelationshipPropertyType(normalizedPropertyType))
  }

  override def semanticCheck: SemanticCheck =
    checkIfExistsDoAndOptions() chain
      checkPropertyTypes(propertyType, normalizedPropertyType) chain
      super.semanticCheck
}

case class DropConstraintOnName(
  name: Expression,
  ifExists: Boolean,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends SchemaCommand {
  override val commandDescription: String = "DROP CONSTRAINT"
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def semanticCheck: SemanticCheck = checkIsStringLiteralOrParameter("constraint name", name)
}

// Graph types

case class AlterCurrentGraphType(
  graphType: GraphType,
  operation: AlterOperation,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends SchemaCommand {
  override val commandDescription: String = "ALTER CURRENT GRAPH TYPE " + operation.name()

  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(
      "`ALTER CURRENT GRAPH TYPE`",
      SemanticFeature.GraphTypes,
      position
    ) chain SemanticCheck.fromState { (state: SemanticState) =>
      SemanticCheck.setState(state.copy(graphTypeMode = operation)) chain graphType.semanticCheck
    }
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
}

object AlterCurrentGraphType {

  sealed trait AlterOperation {
    def name(): String
  }

  case object Set extends AlterOperation {
    override def name(): String = "SET"
  }

  case object Add extends AlterOperation {
    override def name(): String = "ADD"
  }

  case object Drop extends AlterOperation {
    override def name(): String = "DROP"
  }

  case object Alter extends AlterOperation {
    override def name(): String = "ALTER"
  }
}
