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
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType

sealed trait SchemaCommand extends StatementWithGraph with SemanticAnalysisTooling {

  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand

  override def returnColumns: List[LogicalVariable] = List.empty

  override def containsUpdates: Boolean = true

  // The validation of the values (provider, config keys and config values) are done at runtime.
  protected def checkOptionsMap(schemaString: String, options: Options): SemanticCheck = options match {
    case OptionsMap(ops)
      if ops.view.filterKeys(k =>
        !k.equalsIgnoreCase("indexProvider") && !k.equalsIgnoreCase("indexConfig")
      ).nonEmpty =>
      error(
        s"Failed to create $schemaString: Invalid option provided, valid options are `indexProvider` and `indexConfig`.",
        position
      )
    case _ => SemanticCheck.success
  }

  protected def checkSingleProperty(schemaString: String, properties: List[Property]): SemanticCheck =
    when(properties.size > 1) {
      error(s"Only single property $schemaString are supported", properties(1).position)
    }
}

// Indexes

sealed trait CreateIndex extends SchemaCommand {
  // To anonymize the name
  val name: Option[Either[String, Parameter]]
  def withName(name: Option[Either[String, Parameter]]): CreateIndex

  def indexType: CreateIndexType
  def variable: Variable
  def isNodeIndex: Boolean
  def properties: List[Property]
  def ifExistsDo: IfExistsDo
  def options: Options

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace =>
      error("Failed to create index: `OR REPLACE` cannot be used together with this command.", position)
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

object CreateIndex {

  // Help methods for creating the different index types

  def createBtreeNodeIndex(
    variable: Variable,
    label: LabelName,
    properties: List[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = label,
      properties,
      name,
      indexType = BtreeCreateIndex,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createBtreeRelationshipIndex(
    variable: Variable,
    relType: RelTypeName,
    properties: List[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = relType,
      properties,
      name,
      indexType = BtreeCreateIndex,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createFulltextNodeIndex(
    variable: Variable,
    labels: List[LabelName],
    properties: List[Property],
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    label: LabelName,
    properties: List[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = label,
      properties,
      name,
      indexType = VectorCreateIndex,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createVectorRelationshipIndex(
    variable: Variable,
    relType: RelTypeName,
    properties: List[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateIndex =
    CreateSingleLabelPropertyIndexCommand(
      variable,
      entityName = relType,
      properties,
      name,
      indexType = VectorCreateIndex,
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
  }

  override def semanticCheck: SemanticCheck = {
    if (indexType == BtreeCreateIndex)
      error("Invalid index type b-tree, use range, point or text index instead.", position)
    else {
      checkOptionsMap(entityIndexDescription, options) chain
        super.semanticCheck chain {
          if (indexType.singlePropertyOnly) checkSingleProperty(indexType.allDescription, properties)
          else SemanticCheck.success
        }
    }
  }
}

object CreateSingleLabelPropertyIndex {

  def unapply(c: CreateSingleLabelPropertyIndex): Some[(
    Variable,
    ElementTypeName,
    List[Property],
    Option[Either[String, Parameter]],
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
    checkOptionsMap(entityIndexDescription, options) chain super.semanticCheck
}

object CreateFulltextIndex {

  def unapply(c: CreateFulltextIndex): Some[(
    Variable,
    Either[List[LabelName], List[RelTypeName]],
    List[Property],
    Option[Either[String, Parameter]],
    CreateIndexType,
    IfExistsDo,
    Options
  )] =
    Some((c.variable, c.entityNames, c.properties, c.name, c.indexType, c.ifExistsDo, c.options))
}

sealed trait CreateLookupIndex extends CreateIndex {
  def function: FunctionInvocation

  override val indexType: CreateIndexType = LookupCreateIndex
  override val properties: List[Property] = List.empty

  private def allowedFunction(name: String): Boolean =
    if (isNodeIndex) name.equalsIgnoreCase(Labels.name) else name.equalsIgnoreCase(Type.name)

  override def semanticCheck: SemanticCheck = function match {
    case FunctionInvocation(FunctionName(_, name), _, _, _, _) if !allowedFunction(name) =>
      val (validFunction, entityIndexDescription) =
        if (isNodeIndex) (Labels.name, indexType.nodeDescription)
        else (Type.name, indexType.relDescription)

      error(
        s"Failed to create $entityIndexDescription: Function '$name' is not allowed, valid function is '$validFunction'.",
        position
      )
    case _ =>
      checkOptionsMap(indexType.allDescription, options) chain
        super.semanticCheck chain
        SemanticExpressionCheck.simple(function)
  }
}

object CreateLookupIndex {

  def unapply(c: CreateLookupIndex): Some[(
    Variable,
    Boolean,
    FunctionInvocation,
    Option[Either[String, Parameter]],
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
  override val name: Option[Either[String, Parameter]],
  indexType: CreateIndexType,
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateSingleLabelPropertyIndex {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateSingleLabelPropertyIndexCommand =
    copy(name = name)(position)
}

private case class CreateFulltextIndexCommand(
  variable: Variable,
  entityNames: Either[List[LabelName], List[RelTypeName]],
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateFulltextIndex {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateFulltextIndexCommand =
    copy(name = name)(position)
}

private case class CreateLookupIndexCommand(
  variable: Variable,
  isNodeIndex: Boolean,
  function: FunctionInvocation,
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateLookupIndex {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreateLookupIndexCommand = copy(name = name)(position)
}

case class DropIndexOnName(name: Either[String, Parameter], ifExists: Boolean, useGraph: Option[GraphSelection] = None)(
  val position: InputPosition
) extends SchemaCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def semanticCheck: SemanticCheck = Seq()
}

// Constraints

sealed trait CreateConstraint extends SchemaCommand {
  // To anonymize the name
  val name: Option[Either[String, Parameter]]
  def withName(name: Option[Either[String, Parameter]]): CreateConstraint

  def constraintType: CreateConstraintType
  def variable: Variable
  def entityType: CypherType
  def entityName: ElementTypeName
  def properties: Seq[Property]
  def ifExistsDo: IfExistsDo
  def options: Options

  override def semanticCheck: SemanticCheck =
    declareVariable(variable, entityType) chain
      SemanticExpressionCheck.simple(properties) chain
      semanticCheckFold(properties) {
        property =>
          when(!property.map.isInstanceOf[Variable]) {
            error("Cannot index nested properties", property.position)
          }
      }

  protected def checkIfExistsDoAndOptions(): SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace =>
      error(
        s"Failed to create ${constraintType.description} constraint: `OR REPLACE` cannot be used together with this command.",
        position
      )
    case _ =>
      checkOptionsMap(s"${constraintType.description} constraint", options)
  }

  private val allowedPropertyTypes = List(
    BooleanType(isNullable = true)(InputPosition.NONE),
    StringType(isNullable = true)(InputPosition.NONE),
    IntegerType(isNullable = true)(InputPosition.NONE),
    FloatType(isNullable = true)(InputPosition.NONE),
    DateType(isNullable = true)(InputPosition.NONE),
    LocalTimeType(isNullable = true)(InputPosition.NONE),
    ZonedTimeType(isNullable = true)(InputPosition.NONE),
    LocalDateTimeType(isNullable = true)(InputPosition.NONE),
    ZonedDateTimeType(isNullable = true)(InputPosition.NONE),
    DurationType(isNullable = true)(InputPosition.NONE),
    PointType(isNullable = true)(InputPosition.NONE),
    ListType(BooleanType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(StringType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(IntegerType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(FloatType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(DateType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(LocalTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(ZonedTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(LocalDateTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(ZonedDateTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(DurationType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE),
    ListType(PointType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
  )

  protected def checkPropertyTypes(
    originalPropertyType: CypherType,
    normalizedPropertyType: CypherType
  ): SemanticCheck = {

    def allowedTypesCheck = {
      def anyPropertyValueType(pt: CypherType): Boolean = pt match {
        case _: PropertyValueType      => true
        case l: ListType               => anyPropertyValueType(l.innerType)
        case c: ClosedDynamicUnionType => c.sortedInnerTypes.map(anyPropertyValueType).exists(b => b)
        case _                         => false
      }
      val containsPropertyValueType = anyPropertyValueType(originalPropertyType)

      val onlyAllowedTypes = normalizedPropertyType match {
        case c: ClosedDynamicUnionType =>
          c.sortedInnerTypes.forall(p => allowedPropertyTypes.contains(p.withPosition(InputPosition.NONE)))
        case _ =>
          allowedPropertyTypes.contains(normalizedPropertyType.withPosition(InputPosition.NONE))
      }

      if (containsPropertyValueType || !onlyAllowedTypes) {
        def additionalErrorInfo(pt: CypherType): String = pt match {
          case ListType(_: ListType, _) =>
            " Lists cannot have lists as an inner type."
          case ListType(_: ClosedDynamicUnionType, _) =>
            " Lists cannot have a union of types as an inner type."
          case ListType(inner, _) if inner.isNullable =>
            " Lists cannot have nullable inner types."
          case c: ClosedDynamicUnionType if c.sortedInnerTypes.exists(_.isInstanceOf[ListType]) =>
            // If we have lists we want to check them for the above cases as well
            // Unions within unions should have been flattened in parsing so won't be handled here
            c.sortedInnerTypes.filter(_.isInstanceOf[ListType])
              .map(additionalErrorInfo)
              .find(_.nonEmpty)
              .getOrElse("")
          case _ => ""
        }

        // Don't expand the PROPERTY VALUE in error message as that makes it confusing as to why it's not allowed.
        // Similarly, it shouldn't get any additional error messages for being a union in a list,
        // in case of LIST<PROPERTY VALUE>, as that isn't the main reason for failure.
        val (typeDescription, additionalError) =
          if (containsPropertyValueType) (originalPropertyType.description, additionalErrorInfo(originalPropertyType))
          else (normalizedPropertyType.description, additionalErrorInfo(normalizedPropertyType))

        error(
          s"Failed to create ${constraintType.description} constraint: " +
            s"Invalid property type `$typeDescription`.$additionalError",
          originalPropertyType.position
        )
      } else SemanticCheck.success
    }

    // We want run the semantic checks for the types themselves, but the error messages might not make sense in this context
    // There isn't much point telling users to make all their union types NOT NULL if that is not accepted here.
    CypherTypeName(originalPropertyType).semanticCheck.map {
      case r @ SemanticCheckResult(_, Nil) => r
      case SemanticCheckResult(state, _) => SemanticCheckResult(
          state,
          Seq(SemanticError(
            s"Failed to create ${constraintType.description} constraint: " +
              s"Invalid property type `${originalPropertyType.description}`.",
            originalPropertyType.position
          ))
        )
    } chain allowedTypesCheck
  }
}

object CreateConstraint {

  def unapply(c: CreateConstraint): Some[(
    Variable,
    ElementTypeName,
    Seq[Property],
    Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = label,
      properties,
      name,
      constraintType = NodeKey,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createRelationshipKeyConstraint(
    variable: Variable,
    relType: RelTypeName,
    properties: Seq[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = relType,
      properties,
      name,
      constraintType = RelationshipKey,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createNodePropertyUniquenessConstraint(
    variable: Variable,
    label: LabelName,
    properties: Seq[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = label,
      properties,
      name,
      constraintType = NodePropertyUniqueness,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createRelationshipPropertyUniquenessConstraint(
    variable: Variable,
    relType: RelTypeName,
    properties: Seq[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: IfExistsDo,
    options: Options,
    useGraph: Option[GraphSelection] = None
  )(position: InputPosition): CreateConstraint =
    CreateConstraintCommand(
      variable,
      entityName = relType,
      properties,
      name,
      constraintType = RelationshipPropertyUniqueness,
      ifExistsDo,
      options,
      useGraph
    )(position)

  def createNodePropertyExistenceConstraint(
    variable: Variable,
    label: LabelName,
    property: Property,
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
  override val name: Option[Either[String, Parameter]],
  constraintType: CreateConstraintType,
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreateConstraintCommand = copy(name = name)(position)

  val entityType: CypherType = entityName match {
    case _: LabelName   => CTNode
    case _: RelTypeName => CTRelationship
  }

  override def semanticCheck: SemanticCheck = checkIfExistsDoAndOptions() chain super.semanticCheck
}

private case class CreatePropertyTypeConstraint(
  variable: Variable,
  entityName: ElementTypeName,
  property: Property,
  private val propertyType: CypherType,
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreatePropertyTypeConstraint =
    copy(name = name)(position)

  val properties: Seq[Property] = Seq(property)

  // Accessed through the constraint type instead of directly
  private val normalizedPropertyType: CypherType = CypherType.normalizeTypes(propertyType)

  val (entityType: CypherType, constraintType: CreateConstraintType) = entityName match {
    case _: LabelName   => (CTNode, NodePropertyType(normalizedPropertyType))
    case _: RelTypeName => (CTRelationship, RelationshipPropertyType(normalizedPropertyType))
  }

  override def semanticCheck: SemanticCheck =
    checkIfExistsDoAndOptions() chain
      checkPropertyTypes(propertyType, normalizedPropertyType) chain
      super.semanticCheck
}

case class DropConstraintOnName(
  name: Either[String, Parameter],
  ifExists: Boolean,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends SchemaCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def semanticCheck: SemanticCheck = Seq()
}
