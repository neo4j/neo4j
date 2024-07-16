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
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
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

abstract class CreateIndex(
  variable: Variable,
  properties: List[Property],
  ifExistsDo: IfExistsDo,
  isNodeIndex: Boolean
)(val position: InputPosition)
    extends SchemaCommand {

  // To anonymize the name
  val name: Option[Either[String, Parameter]]
  def withName(name: Option[Either[String, Parameter]]): CreateIndex

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

case class CreateBtreeNodeIndex(
  variable: Variable,
  label: LabelName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreateBtreeNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    error("Invalid index type b-tree, use range, point or text index instead.", position)
}

case class CreateBtreeRelationshipIndex(
  variable: Variable,
  relType: RelTypeName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateBtreeRelationshipIndex =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    error("Invalid index type b-tree, use range, point or text index instead.", position)
}

case class CreateRangeNodeIndex(
  variable: Variable,
  label: LabelName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  fromDefault: Boolean,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreateRangeNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("range node property index", options) chain super.semanticCheck
}

case class CreateRangeRelationshipIndex(
  variable: Variable,
  relType: RelTypeName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  fromDefault: Boolean,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateRangeRelationshipIndex =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("range relationship property index", options) chain super.semanticCheck
}

case class CreateLookupIndex(
  variable: Variable,
  isNodeIndex: Boolean,
  function: FunctionInvocation,
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, List.empty, ifExistsDo, isNodeIndex)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreateLookupIndex = copy(name = name)(position)

  private def allowedFunction(name: String): Boolean =
    if (isNodeIndex) name.equalsIgnoreCase(Labels.name) else name.equalsIgnoreCase(Type.name)

  override def semanticCheck: SemanticCheck = function match {
    case FunctionInvocation(FunctionName(_, name), _, _, _, _) if !allowedFunction(name) =>
      if (isNodeIndex) error(
        s"Failed to create node lookup index: Function '$name' is not allowed, valid function is '${Labels.name}'.",
        position
      )
      else error(
        s"Failed to create relationship lookup index: Function '$name' is not allowed, valid function is '${Type.name}'.",
        position
      )
    case _ =>
      checkOptionsMap("token lookup index", options) chain super.semanticCheck chain SemanticExpressionCheck.simple(
        function
      )
  }
}

case class CreateFulltextNodeIndex(
  variable: Variable,
  label: List[LabelName],
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreateFulltextNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck = checkOptionsMap("fulltext node index", options) chain super.semanticCheck
}

case class CreateFulltextRelationshipIndex(
  variable: Variable,
  relType: List[RelTypeName],
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateFulltextRelationshipIndex =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("fulltext relationship index", options) chain super.semanticCheck
}

case class CreateTextNodeIndex(
  variable: Variable,
  label: LabelName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreateTextNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("text node index", options) chain
      super.semanticCheck chain
      checkSingleProperty("text indexes", properties)
}

case class CreateTextRelationshipIndex(
  variable: Variable,
  relType: RelTypeName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateTextRelationshipIndex =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("text relationship index", options) chain
      super.semanticCheck chain
      checkSingleProperty("text indexes", properties)
}

case class CreatePointNodeIndex(
  variable: Variable,
  label: LabelName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreatePointNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("point node index", options) chain
      super.semanticCheck chain
      checkSingleProperty("point indexes", properties)
}

case class CreatePointRelationshipIndex(
  variable: Variable,
  relType: RelTypeName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreatePointRelationshipIndex =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("point relationship index", options) chain
      super.semanticCheck chain
      checkSingleProperty("point indexes", properties)
}

case class CreateVectorNodeIndex(
  variable: Variable,
  label: LabelName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, true)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreateVectorNodeIndex = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("vector node index", options) chain
      super.semanticCheck chain
      checkSingleProperty("vector indexes", properties)
}

case class CreateVectorRelationshipIndex(
  variable: Variable,
  relType: RelTypeName,
  properties: List[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(override val position: InputPosition)
    extends CreateIndex(variable, properties, ifExistsDo, false)(position) {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateVectorRelationshipIndex =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkOptionsMap("vector relationship index", options) chain
      super.semanticCheck chain
      checkSingleProperty("vector indexes", properties)
}

case class DropIndexOnName(name: Either[String, Parameter], ifExists: Boolean, useGraph: Option[GraphSelection] = None)(
  val position: InputPosition
) extends SchemaCommand {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def semanticCheck: SemanticCheck = Seq()
}

// Constraints

sealed protected trait PropertyConstraintCommand extends SchemaCommand {
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

sealed protected trait CompositePropertyConstraintCommand extends SchemaCommand {
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

sealed protected trait NodePropertyConstraintCommand extends PropertyConstraintCommand {

  val entityType: NodeType = CTNode

  def label: LabelName
}

sealed protected trait NodeCompositePropertyConstraintCommand extends CompositePropertyConstraintCommand {

  val entityType: NodeType = CTNode

  def label: LabelName
}

sealed protected trait RelationshipPropertyConstraintCommand extends PropertyConstraintCommand {

  val entityType: RelationshipType = CTRelationship

  def relType: RelTypeName
}

sealed protected trait RelationshipCompositePropertyConstraintCommand extends CompositePropertyConstraintCommand {

  val entityType: RelationshipType = CTRelationship

  def relType: RelTypeName
}

sealed trait CreateConstraint extends SchemaCommand {
  // To anonymize the name
  val name: Option[Either[String, Parameter]]
  def withName(name: Option[Either[String, Parameter]]): CreateConstraint

  protected def checkSemantics(
    constraintTypeString: String,
    ifExistsDo: IfExistsDo,
    options: Options
  ): SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax | IfExistsReplace =>
      error(
        s"Failed to create $constraintTypeString constraint: `OR REPLACE` cannot be used together with this command.",
        position
      )
    case _ =>
      checkOptionsMap(s"$constraintTypeString constraint", options)
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
    entityTypeString: String,
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
          s"Failed to create $entityTypeString property type constraint: " +
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
            s"Failed to create $entityTypeString property type constraint: " +
              s"Invalid property type `${originalPropertyType.description}`.",
            originalPropertyType.position
          ))
        )
    } chain allowedTypesCheck
  }
}

case class CreateNodeKeyConstraint(
  variable: Variable,
  label: LabelName,
  properties: Seq[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends NodeCompositePropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)
  override def withName(name: Option[Either[String, Parameter]]): CreateNodeKeyConstraint = copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkSemantics("node key", ifExistsDo, options) chain super.semanticCheck
}

case class CreateRelationshipKeyConstraint(
  variable: Variable,
  relType: RelTypeName,
  properties: Seq[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends RelationshipCompositePropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateRelationshipKeyConstraint =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkSemantics("relationship key", ifExistsDo, options) chain super.semanticCheck
}

case class CreateNodePropertyUniquenessConstraint(
  variable: Variable,
  label: LabelName,
  properties: Seq[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends NodeCompositePropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateNodePropertyUniquenessConstraint =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkSemantics("uniqueness", ifExistsDo, options) chain super.semanticCheck
}

case class CreateRelationshipPropertyUniquenessConstraint(
  variable: Variable,
  relType: RelTypeName,
  properties: Seq[Property],
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends RelationshipCompositePropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateRelationshipPropertyUniquenessConstraint =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkSemantics("relationship uniqueness", ifExistsDo, options) chain super.semanticCheck
}

case class CreateNodePropertyExistenceConstraint(
  variable: Variable,
  label: LabelName,
  property: Property,
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends NodePropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateNodePropertyExistenceConstraint =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkSemantics("node property existence", ifExistsDo, options) chain super.semanticCheck
}

case class CreateRelationshipPropertyExistenceConstraint(
  variable: Variable,
  relType: RelTypeName,
  property: Property,
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends RelationshipPropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateRelationshipPropertyExistenceConstraint =
    copy(name = name)(position)

  override def semanticCheck: SemanticCheck =
    checkSemantics("relationship property existence", ifExistsDo, options) chain super.semanticCheck
}

case class CreateNodePropertyTypeConstraint(
  variable: Variable,
  label: LabelName,
  property: Property,
  private val propertyType: CypherType,
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends NodePropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateNodePropertyTypeConstraint =
    copy(name = name)(position)

  val normalizedPropertyType: CypherType = CypherType.normalizeTypes(propertyType)

  override def semanticCheck: SemanticCheck =
    checkSemantics("node property type", ifExistsDo, options) chain
      checkPropertyTypes("node", propertyType, normalizedPropertyType) chain
      super.semanticCheck
}

case class CreateRelationshipPropertyTypeConstraint(
  variable: Variable,
  relType: RelTypeName,
  property: Property,
  private val propertyType: CypherType,
  override val name: Option[Either[String, Parameter]],
  ifExistsDo: IfExistsDo,
  options: Options,
  useGraph: Option[GraphSelection] = None
)(val position: InputPosition) extends RelationshipPropertyConstraintCommand with CreateConstraint {
  override def withGraph(useGraph: Option[UseGraph]): SchemaCommand = copy(useGraph = useGraph)(position)

  override def withName(name: Option[Either[String, Parameter]]): CreateRelationshipPropertyTypeConstraint =
    copy(name = name)(position)

  val normalizedPropertyType: CypherType = CypherType.normalizeTypes(propertyType)

  override def semanticCheck: SemanticCheck =
    checkSemantics("relationship property type", ifExistsDo, options) chain
      checkPropertyTypes("relationship", propertyType, normalizedPropertyType) chain
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
