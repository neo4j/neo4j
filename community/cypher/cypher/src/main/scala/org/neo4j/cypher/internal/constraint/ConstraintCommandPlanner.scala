/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.constraint

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.SchemaCommandRuntime.checkTypes
import org.neo4j.cypher.internal.SchemaCommandRuntime.getEntityInfo
import org.neo4j.cypher.internal.SchemaCommandRuntime.getName
import org.neo4j.cypher.internal.SchemaCommandRuntime.getPrettyEntityPattern
import org.neo4j.cypher.internal.SchemaCommandRuntime.getPrettyName
import org.neo4j.cypher.internal.SchemaCommandRuntime.getPrettyPropertyPattern
import org.neo4j.cypher.internal.SchemaCommandRuntime.indexContext
import org.neo4j.cypher.internal.SchemaCommandRuntime.propertyToId
import org.neo4j.cypher.internal.ast.CreateConstraintType
import org.neo4j.cypher.internal.ast.NodeKey
import org.neo4j.cypher.internal.ast.NodePropertyExistence
import org.neo4j.cypher.internal.ast.NodePropertyType
import org.neo4j.cypher.internal.ast.NodePropertyUniqueness
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.RelationshipKey
import org.neo4j.cypher.internal.ast.RelationshipPropertyExistence
import org.neo4j.cypher.internal.ast.RelationshipPropertyType
import org.neo4j.cypher.internal.ast.RelationshipPropertyUniqueness
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.notification.IndexOrConstraintAlreadyExistsNotification
import org.neo4j.cypher.internal.notification.IndexOrConstraintDoesNotExistNotification
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.optionsmap.IndexBackedConstraintsOptionsConverter
import org.neo4j.cypher.internal.optionsmap.PropertyExistenceOrTypeConstraintOptionsConverter
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescription.prettyOptions
import org.neo4j.cypher.internal.plandescription.asPrettyString
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringInterpolator
import org.neo4j.cypher.internal.procs.IgnoredResult
import org.neo4j.cypher.internal.procs.PropertyTypeMapper
import org.neo4j.cypher.internal.procs.SchemaExecutionResult
import org.neo4j.cypher.internal.procs.SuccessResult
import org.neo4j.cypher.internal.runtime.ConstraintInformation
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.ConstraintType.EXISTS
import org.neo4j.internal.schema.ConstraintType.NODE_LABEL_EXISTENCE
import org.neo4j.internal.schema.ConstraintType.PROPERTY_TYPE
import org.neo4j.internal.schema.ConstraintType.RELATIONSHIP_ENDPOINT_LABEL
import org.neo4j.internal.schema.ConstraintType.UNIQUE
import org.neo4j.internal.schema.ConstraintType.UNIQUE_EXISTS
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create
import org.neo4j.util.Stringifier
import org.neo4j.values.virtual.MapValue

import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * Create the schema write functions for the community and enterprise constraint command SchemaExecutionPlan's.
 * The enterprise commands are just here to not change the existing errors for them.
 */
object ConstraintCommandPlanner {

  // Create methods

  def createNodeKeyConstraint(
    nodeKey: NodeKey,
    label: LabelName,
    props: Seq[Property],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      val (maybeIndexProvider, notifications) =
        IndexBackedConstraintsOptionsConverter(s"${nodeKey.description} constraint", indexContext(ctx))
          .convert(cypherVersion, options, params, Some(ctx.getConfig))
          .toOptionNotification
      val indexProvider = maybeIndexProvider.flatMap(_.provider)
      val propertyKeys = props.map(p => p.propertyKey.name)
      ctx.createConstraint(new Create.NodeKey(
        constraintName.orNull,
        label.name,
        propertyKeys.asJava,
        indexProvider.orNull,
        false
      ))
      SuccessResult(notifications)
    }

  def createRelationshipKeyConstraint(
    relKey: RelationshipKey,
    relType: RelTypeName,
    props: Seq[Property],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      val (maybeIndexProvider, notifications) =
        IndexBackedConstraintsOptionsConverter(s"${relKey.description} constraint", indexContext(ctx))
          .convert(cypherVersion, options, params, Some(ctx.getConfig))
          .toOptionNotification
      val indexProvider = maybeIndexProvider.flatMap(_.provider)
      val propertyKeys = props.map(p => p.propertyKey.name)
      ctx.createConstraint(new Create.RelationshipKey(
        constraintName.orNull,
        relType.name,
        propertyKeys.asJava,
        indexProvider.orNull,
        false
      ))
      SuccessResult(notifications)
    }

  def createNodePropertyUniquenessConstraint(
    nodePropUnique: NodePropertyUniqueness,
    label: LabelName,
    props: Seq[Property],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      val (maybeIndexProvider, notifications) =
        IndexBackedConstraintsOptionsConverter(s"${nodePropUnique.description} constraint", indexContext(ctx))
          .convert(cypherVersion, options, params, Some(ctx.getConfig))
          .toOptionNotification
      val indexProvider = maybeIndexProvider.flatMap(_.provider)
      val propertyKeys = props.map(p => p.propertyKey.name)
      ctx.createConstraint(new Create.NodeUniqueness(
        constraintName.orNull,
        label.name,
        propertyKeys.asJava,
        indexProvider.orNull,
        false
      ))
      SuccessResult(notifications)
    }

  def createRelationshipPropertyUniquenessConstraint(
    relPropUnique: RelationshipPropertyUniqueness,
    relType: RelTypeName,
    props: Seq[Property],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      val (maybeIndexProvider, notifications) =
        IndexBackedConstraintsOptionsConverter(s"${relPropUnique.description} constraint", indexContext(ctx))
          .convert(cypherVersion, options, params, Some(ctx.getConfig))
          .toOptionNotification
      val indexProvider = maybeIndexProvider.flatMap(_.provider)
      val propertyKeys = props.map(p => p.propertyKey.name)
      ctx.createConstraint(new Create.RelationshipUniqueness(
        constraintName.orNull,
        relType.name,
        propertyKeys.asJava,
        indexProvider.orNull,
        false
      ))
      SuccessResult(notifications)
    }

  def createNodePropertyExistenceConstraint(
    label: LabelName,
    prop: Seq[Property],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      // Assert empty options
      PropertyExistenceOrTypeConstraintOptionsConverter("node", "existence", indexContext(ctx))
        .convert(cypherVersion, options, params)
      ctx.createConstraint(new Create.NodeExistence(
        constraintName.orNull,
        label.name,
        prop.head.propertyKey.name,
        false,
        false
      ))
      SuccessResult()
    }

  def createRelationshipPropertyExistenceConstraint(
    relType: RelTypeName,
    prop: Seq[Property],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      // Assert empty options
      PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "existence", indexContext(ctx))
        .convert(cypherVersion, options, params)
      ctx.createConstraint(new Create.RelationshipExistence(
        constraintName.orNull,
        relType.name,
        prop.head.propertyKey.name,
        false,
        false
      ))
      SuccessResult()
    }

  def createNodePropertyTypeConstraint(
    propertyType: CypherType,
    label: LabelName,
    prop: Seq[Property],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      // Assert empty options
      PropertyExistenceOrTypeConstraintOptionsConverter("node", "type", indexContext(ctx))
        .convert(cypherVersion, options, params)
      ctx.createConstraint(new Create.NodePropertyType(
        constraintName.orNull,
        label.name,
        prop.head.propertyKey.name,
        PropertyTypeMapper.asPropertyTypeSet(propertyType),
        false,
        false
      ))
      SuccessResult()
    }

  def createRelationshipPropertyTypeConstraint(
    propertyType: CypherType,
    relType: RelTypeName,
    prop: Seq[Property],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      // Assert empty options
      PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "type", indexContext(ctx))
        .convert(cypherVersion, options, params)
      ctx.createConstraint(new Create.RelationshipPropertyType(
        constraintName.orNull,
        relType.name,
        prop.head.propertyKey.name,
        PropertyTypeMapper.asPropertyTypeSet(propertyType),
        false,
        false
      ))
      SuccessResult()
    }

  // Drop methods

  def dropConstraint(
    name: Expression,
    ifExists: Boolean
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      val notifications: Set[InternalNotification] = if (!ifExists || ctx.constraintExists(constraintName)) {
        ctx.dropNamedConstraint(constraintName, allowDependent = false)
        Set.empty
      } else {
        // Notify on non-existing constraint, replace potential parameter names with their actual value
        Set(IndexOrConstraintDoesNotExistNotification(
          s"DROP CONSTRAINT ${Stringifier.backtickEmpty(constraintName)} IF EXISTS",
          constraintName
        ))
      }
      SuccessResult(notifications)
    }

  // If exists methods

  def doNothingIfExists(
    entityName: ElementTypeName,
    props: Seq[Property],
    assertion: CreateConstraintType,
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val constraintName = getName(name, params)
      // Assert correct options to get errors even if matching constraint already exists
      val conversionResult = assertion match {
        case nodeKey: NodeKey =>
          IndexBackedConstraintsOptionsConverter(s"${nodeKey.description} constraint", indexContext(ctx))
            .convert(cypherVersion, options, params, Some(ctx.getConfig))
        case relKey: RelationshipKey =>
          IndexBackedConstraintsOptionsConverter(s"${relKey.description} constraint", indexContext(ctx))
            .convert(cypherVersion, options, params, Some(ctx.getConfig))
        case nodePropUnique: NodePropertyUniqueness =>
          IndexBackedConstraintsOptionsConverter(s"${nodePropUnique.description} constraint", indexContext(ctx))
            .convert(cypherVersion, options, params, Some(ctx.getConfig))
        case relPropUnique: RelationshipPropertyUniqueness =>
          IndexBackedConstraintsOptionsConverter(s"${relPropUnique.description} constraint", indexContext(ctx))
            .convert(cypherVersion, options, params, Some(ctx.getConfig))
        case NodePropertyExistence =>
          PropertyExistenceOrTypeConstraintOptionsConverter("node", "existence", indexContext(ctx))
            .convert(cypherVersion, options, params)
        case RelationshipPropertyExistence =>
          PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "existence", indexContext(ctx))
            .convert(cypherVersion, options, params)
        case _: NodePropertyType =>
          PropertyExistenceOrTypeConstraintOptionsConverter("node", "type", indexContext(ctx))
            .convert(cypherVersion, options, params)
        case _: RelationshipPropertyType =>
          PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "type", indexContext(ctx))
            .convert(cypherVersion, options, params)
      }

      val optionConverterNotifications = conversionResult.toOptionNotification._2

      val (entityId, _) = getEntityInfo(entityName, ctx)
      val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
      val constraintMatcher = convertConstraintTypeToConstraintMatcher(assertion)
      if (ctx.constraintExists(constraintMatcher, entityId, propertyKeyIds: _*)) {
        // Notify on pre-existing constraint, replace potential parameter names with their actual value
        val constraintDescription = constraintInfo(constraintName, entityName, props, assertion, options)
        val conflictingConstraint = existingConstraintInfo(
          ctx,
          () => ctx.getConstraintInformation(constraintMatcher, entityId, propertyKeyIds: _*),
          cypherVersion
        )

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $constraintDescription",
          conflictingConstraint
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else if (constraintName.exists(ctx.constraintExists)) {
        // Notify on pre-existing constraint, replace potential parameter names with their actual value
        val constraintDescription = constraintInfo(constraintName, entityName, props, assertion, options)
        val conflictingConstraint = existingConstraintInfo(
          ctx,
          () => ctx.getConstraintInformation(constraintName.get),
          cypherVersion
        )

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $constraintDescription",
          conflictingConstraint
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else {
        SuccessResult(optionConverterNotifications)
      }
    }

  // Help methods

  private def convertConstraintTypeToConstraintMatcher(
    assertion: CreateConstraintType
  ): ConstraintDescriptor => Boolean =
    assertion match {
      case NodePropertyExistence             => c => c.isNodePropertyExistenceConstraint
      case RelationshipPropertyExistence     => c => c.isRelationshipPropertyExistenceConstraint
      case _: NodePropertyUniqueness         => c => c.isNodeUniquenessConstraint
      case _: RelationshipPropertyUniqueness => c => c.isRelationshipUniquenessConstraint
      case _: NodeKey                        => c => c.isNodeKeyConstraint
      case _: RelationshipKey                => c => c.isRelationshipKeyConstraint
      case NodePropertyType(propType) =>
        c => c.isNodePropertyTypeConstraint && checkTypes(propType, c.asPropertyTypeConstraint().propertyType())
      case RelationshipPropertyType(propType) =>
        c =>
          c.isRelationshipPropertyTypeConstraint &&
            checkTypes(propType, c.asPropertyTypeConstraint().propertyType())
    }

  private def constraintInfo(
    nameOption: Option[String],
    entityName: ElementTypeName,
    properties: Seq[Property],
    constraintType: CreateConstraintType,
    options: Options
  ): String = {
    val name = getPrettyName(nameOption)
    val pattern = getPrettyEntityPattern(entityName)
    val propertyString = getPrettyPropertyPattern(properties.map(p => p.propertyKey), "(", ")")
    val prettyAssertion = asPrettyString.raw(constraintType.predicate)
    pretty"CONSTRAINT$name IF NOT EXISTS FOR $pattern REQUIRE $propertyString $prettyAssertion${prettyOptions(options)}".prettifiedString
  }

  private def existingConstraintInfo(
    ctx: QueryContext,
    getInfoParts: () => ConstraintInformation,
    cypherVersion: CypherVersion
  ): String = {
    try {
      // Assert we are allowed to see the constraint description
      ctx.assertShowConstraintAllowed()

      // Fetch the relevant constraint parts
      val ConstraintInformation(
        isNode,
        constraintType,
        name,
        entityName,
        properties,
        propertyType,
        impliedLabel,
        forSourceNode
      ) = getInfoParts()

      // Create string description
      val nameString = getPrettyName(Some(name))
      val pattern = getPrettyEntityPattern(isNode, entityName)
      val propertyString = getPrettyPropertyPattern(properties, "(", ")")
      val assertion = constraintType match {
        case EXISTS => "IS NOT NULL"
        case UNIQUE_EXISTS =>
          if (cypherVersion == CypherVersion.Cypher5)
            if (isNode) "IS NODE KEY" else "IS RELATIONSHIP KEY"
          else "IS KEY"
        case UNIQUE                      => "IS UNIQUE"
        case PROPERTY_TYPE               => s"IS :: ${propertyType.get}"
        case RELATIONSHIP_ENDPOINT_LABEL => ""
        case NODE_LABEL_EXISTENCE        => ""
      }
      val prettyAssertion = asPrettyString.raw(assertion)
      // We don't have a constraint command for endpoint and node label existence constraints,
      // lets use `(:Label1 => :Label2)`, `(:Label)-[:REL_TYPE =>]->()` and `()-[:REL_TYPE =>]->(:Label)` with correct labels and relationship types
      // as they don't have constraint commands and that would be their representation in the graph type.
      if (constraintType == RELATIONSHIP_ENDPOINT_LABEL)
        if (forSourceNode.get)
          pretty"(:${asPrettyString(impliedLabel.get)})-[:${asPrettyString(entityName)} =>]->()".prettifiedString
        else pretty"()-[:${asPrettyString(entityName)} =>]->(:${asPrettyString(impliedLabel.get)})".prettifiedString
      else if (constraintType == NODE_LABEL_EXISTENCE)
        pretty"(:${asPrettyString(entityName)} => :${asPrettyString(impliedLabel.get)})".prettifiedString
      else pretty"CONSTRAINT$nameString FOR $pattern REQUIRE $propertyString $prettyAssertion".prettifiedString
    } catch {
      // Not allowed to see constraint description, only show `constraint`
      case _: AuthorizationViolationException => "constraint"
    }
  }
}
