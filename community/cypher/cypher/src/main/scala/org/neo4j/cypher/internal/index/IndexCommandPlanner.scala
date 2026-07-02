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
package org.neo4j.cypher.internal.index

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.SchemaCommandRuntime.getEntityInfo
import org.neo4j.cypher.internal.SchemaCommandRuntime.getName
import org.neo4j.cypher.internal.SchemaCommandRuntime.getPrettyEntityPattern
import org.neo4j.cypher.internal.SchemaCommandRuntime.getPrettyName
import org.neo4j.cypher.internal.SchemaCommandRuntime.getPrettyPropertyPattern
import org.neo4j.cypher.internal.SchemaCommandRuntime.indexContext
import org.neo4j.cypher.internal.SchemaCommandRuntime.propertyToId
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.notification.IndexOrConstraintAlreadyExistsNotification
import org.neo4j.cypher.internal.notification.IndexOrConstraintDoesNotExistNotification
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.optionsmap.CreateFulltextIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateIndexProviderOnlyOptions
import org.neo4j.cypher.internal.optionsmap.CreateIndexWithFullOptions
import org.neo4j.cypher.internal.optionsmap.CreateLookupIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreatePointIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateRangeIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateTextIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateVectorIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.IndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.Nothing
import org.neo4j.cypher.internal.optionsmap.ParsedOptions
import org.neo4j.cypher.internal.optionsmap.ParsedWithNotifications
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescription.prettyOptions
import org.neo4j.cypher.internal.plandescription.PrettyString
import org.neo4j.cypher.internal.plandescription.asPrettyString
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringInterpolator
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringMaker
import org.neo4j.cypher.internal.procs.IgnoredResult
import org.neo4j.cypher.internal.procs.SchemaExecutionResult
import org.neo4j.cypher.internal.procs.SuccessResult
import org.neo4j.cypher.internal.runtime.IndexInformation
import org.neo4j.cypher.internal.runtime.IndexProviderContext
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.graphdb.schema.IndexType.POINT
import org.neo4j.graphdb.schema.IndexType.RANGE
import org.neo4j.graphdb.schema.IndexType.TEXT
import org.neo4j.graphdb.schema.IndexType.VECTOR
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.schema
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion
import org.neo4j.util.Stringifier
import org.neo4j.values.virtual.MapValue

import scala.util.Try

/**
 * Create the schema write functions for the index command SchemaExecutionPlan's
 */
object IndexCommandPlanner {

  // Create methods

  def createFulltextIndex(
    entityNames: Either[List[LabelName], List[RelTypeName]],
    props: List[PropertyKeyName],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      val (indexProvider, indexConfig, notifications) =
        CreateFulltextIndexOptionsConverter(indexContext(ctx))
          .convert(cypherVersion, options, params, Some(ctx.getConfig)) match {
          case Nothing => (None, schema.IndexConfig.empty(), Set.empty[InternalNotification])
          case ParsedOptions(CreateIndexWithFullOptions(provider, config)) =>
            (provider, config, Set.empty[InternalNotification])
          case ParsedWithNotifications(CreateIndexWithFullOptions(provider, config), notifications) =>
            (provider, config, notifications)
        }
      val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
      val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
      ctx.addFulltextIndexRule(entityIds, entityType, propertyKeyIds, indexName, indexProvider, indexConfig)
      SuccessResult(notifications)
    }

  def createLookupIndex(
    entityType: EntityType,
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      val (maybeProvider, notifications) = CreateLookupIndexOptionsConverter(indexContext(ctx))
        .convert(cypherVersion, options, params, Some(ctx.getConfig))
        .toOptionNotification
      val provider = maybeProvider.flatMap(_.provider)
      ctx.addLookupIndexRule(entityType, indexName, provider)
      SuccessResult(notifications)
    }

  def createPointIndex(
    entityName: ElementTypeName,
    props: List[PropertyKeyName],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      val (indexProvider, indexConfig, notifications) =
        CreatePointIndexOptionsConverter(indexContext(ctx))
          .convert(cypherVersion, options, params, Some(ctx.getConfig)) match {
          case Nothing => (None, schema.IndexConfig.empty(), Set.empty[InternalNotification])
          case ParsedOptions(CreateIndexWithFullOptions(provider, config)) =>
            (provider, config, Set.empty[InternalNotification])
          case ParsedWithNotifications(CreateIndexWithFullOptions(provider, config), notifications) =>
            (provider, config, notifications)
        }
      val (entityId, entityType) = getEntityInfo(entityName, ctx)
      val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
      ctx.addPointIndexRule(entityId, entityType, propertyKeyIds, indexName, indexProvider, indexConfig)
      SuccessResult(notifications)
    }

  def createRangeIndex(
    entityName: ElementTypeName,
    props: List[PropertyKeyName],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      val (entityId, entityType) = getEntityInfo(entityName, ctx)
      val schemaType = entityType match {
        case EntityType.NODE         => "range node property index"
        case EntityType.RELATIONSHIP => "range relationship property index"
      }
      val (maybeProvider, notifications) =
        CreateRangeIndexOptionsConverter(schemaType, indexContext(ctx))
          .convert(cypherVersion, options, params, Some(ctx.getConfig))
          .toOptionNotification
      val provider = maybeProvider.flatMap(_.provider)
      val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
      ctx.addRangeIndexRule(entityId, entityType, propertyKeyIds, indexName, provider)
      SuccessResult(notifications)
    }

  def createTextIndex(
    entityName: ElementTypeName,
    props: List[PropertyKeyName],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      val (maybeProvider: Option[CreateIndexProviderOnlyOptions], notifications) =
        CreateTextIndexOptionsConverter(indexContext(ctx))
          .convert(cypherVersion, options, params, Some(ctx.getConfig))
          .toOptionNotification
      val provider = maybeProvider.flatMap(_.provider)
      val (entityId, entityType) = getEntityInfo(entityName, ctx)
      val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
      ctx.addTextIndexRule(entityId, entityType, propertyKeyIds, indexName, provider)
      SuccessResult(notifications)
    }

  def createVectorIndex(
    entityNames: Either[List[LabelName], List[RelTypeName]],
    props: List[PropertyKeyName],
    additionalProps: List[PropertyKeyName],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      val (indexProvider, indexConfig, notifications) =
        CreateVectorIndexOptionsConverter(indexContext(ctx), vectorIndexVersion(ctx))
          .convert(cypherVersion, options, params, Some(ctx.getConfig)) match {
          case Nothing =>
            (None, schema.IndexConfig.empty(), Set.empty[InternalNotification])
          case ParsedOptions(CreateIndexWithFullOptions(provider, config)) =>
            (provider, config, Set.empty[InternalNotification])
          case ParsedWithNotifications(CreateIndexWithFullOptions(provider, config), notifications) =>
            (provider, config, notifications)
        }
      val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
      val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
      val additionalPropertyKeyIds = additionalProps.map(p => propertyToId(ctx)(p).id)
      ctx.addVectorIndexRule(
        entityIds,
        entityType,
        propertyKeyIds,
        additionalPropertyKeyIds,
        indexName,
        indexProvider,
        indexConfig
      )
      SuccessResult(notifications)
    }

  // Drop methods

  def dropIndex(
    name: Expression,
    ifExists: Boolean
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      val notifications: Set[InternalNotification] = if (!ifExists || ctx.indexExists(indexName)) {
        ctx.dropIndexRule(indexName)
        Set.empty
      } else {
        // Notify on non-existing index, replace potential parameter names with their actual value
        Set(IndexOrConstraintDoesNotExistNotification(
          s"DROP INDEX ${Stringifier.backtickEmpty(indexName)} IF EXISTS",
          indexName
        ))
      }
      SuccessResult(notifications)
    }

  // If exists methods

  def doNothingIfExists(
    entityName: ElementTypeName,
    propertyKeyNames: List[PropertyKeyName],
    indexType: IndexType,
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult = {
    val (innerIndexType, optionsConverter): (schema.IndexType, QueryContext => IndexOptionsConverter[?]) =
      indexType match {
        case POINT => (schema.IndexType.POINT, CreatePointIndexOptionsConverter.apply)
        case RANGE => (
            schema.IndexType.RANGE,
            (ctx: QueryContext) =>
              CreateRangeIndexOptionsConverter("range index", indexContext(ctx))
          )
        case TEXT => (schema.IndexType.TEXT, CreateTextIndexOptionsConverter.apply)
        case VECTOR => (
            schema.IndexType.VECTOR,
            (ctx: QueryContext) =>
              CreateVectorIndexOptionsConverter(indexContext(ctx), vectorIndexVersion(ctx))
          )
        case it =>
          throw InternalException.internalError(
            this.getClass.getSimpleName,
            s"Unexpected index type, expected point, range or text. Got: $it.",
            s"Did not expect index type $it here: only point, range or text indexes."
          )
      }
    (ctx, params) => {
      val indexName = getName(name, params)
      // Assert correct options to get errors even if matching index already exists
      val optionConverterNotifications = optionsConverter(ctx)
        .convert(cypherVersion, options, params, Some(ctx.getConfig))
        .toOptionNotification
        ._2

      val (entityId, entityType) = getEntityInfo(entityName, ctx)
      val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
      val existingIndexDescriptor =
        Try(ctx.indexReference(innerIndexType, entityId, entityType, propertyKeyIds: _*))
      if (existingIndexDescriptor.isSuccess) {
        // Notify on pre-existing index, replace potential parameter names with their actual value
        val indexDescription = indexInfo(indexType.name(), indexName, entityName, propertyKeyNames, options)
        val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(existingIndexDescriptor.get))

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $indexDescription",
          conflictingIndex
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else if (indexName.exists(ctx.indexExists)) {
        // Notify on pre-existing index, replace potential parameter names with their actual value
        val indexDescription = indexInfo(indexType.name(), indexName, entityName, propertyKeyNames, options)
        val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(indexName.get))

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $indexDescription",
          conflictingIndex
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else {
        SuccessResult(optionConverterNotifications)
      }
    }
  }

  def doNothingIfExistsForFulltext(
    entityNames: Either[List[LabelName], List[RelTypeName]],
    propertyKeyNames: List[PropertyKeyName],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      // Assert correct options to get errors even if matching index already exists
      val optionConverterNotifications = CreateFulltextIndexOptionsConverter(ctx)
        .convert(cypherVersion, options, params, Some(ctx.getConfig))
        .toOptionNotification
        ._2

      val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
      val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
      val existingIndexDescriptor = Try(ctx.semanticIndexReference(
        schema.IndexType.FULLTEXT,
        entityIds,
        entityType,
        propertyKeyIds: _*
      ))
      if (existingIndexDescriptor.isSuccess) {
        // Notify on pre-existing index, replace potential parameter names with their actual value
        val indexDescription = fulltextIndexInfo(indexName, entityNames, propertyKeyNames, options)
        val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(existingIndexDescriptor.get))

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $indexDescription",
          conflictingIndex
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else if (indexName.exists(ctx.indexExists)) {
        // Notify on pre-existing index, replace potential parameter names with their actual value
        val indexDescription = fulltextIndexInfo(indexName, entityNames, propertyKeyNames, options)
        val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(indexName.get))

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $indexDescription",
          conflictingIndex
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else {
        SuccessResult(optionConverterNotifications)
      }
    }

  def doNothingIfExistsForVector(
    entityNames: Either[List[LabelName], List[RelTypeName]],
    propertyKeyNames: List[PropertyKeyName],
    additionalPropertyKeyNames: List[PropertyKeyName],
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      // Assert correct options to get errors even if matching index already exists
      val optionConverterNotifications = CreateVectorIndexOptionsConverter(indexContext(ctx), vectorIndexVersion(ctx))
        .convert(cypherVersion, options, params, Some(ctx.getConfig))
        .toOptionNotification
        ._2

      val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
      val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
      val additionalPropertyKeyIds = additionalPropertyKeyNames.map(p => propertyToId(ctx)(p).id)
      // Kernel only sees it as a single list with the vector property first
      val allPropertyKeyIds = propertyKeyIds ++ additionalPropertyKeyIds
      val existingIndexDescriptor = Try(ctx.semanticIndexReference(
        schema.IndexType.VECTOR,
        entityIds,
        entityType,
        allPropertyKeyIds: _*
      ))
      if (existingIndexDescriptor.isSuccess) {
        // Notify on pre-existing index, replace potential parameter names with their actual value
        val indexDescription =
          vectorIndexInfo(indexName, entityNames, propertyKeyNames, additionalPropertyKeyNames, options)
        val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(existingIndexDescriptor.get))

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $indexDescription",
          conflictingIndex
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else if (indexName.exists(ctx.indexExists)) {
        // Notify on pre-existing index, replace potential parameter names with their actual value
        val indexDescription =
          vectorIndexInfo(indexName, entityNames, propertyKeyNames, additionalPropertyKeyNames, options)
        val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(indexName.get))

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $indexDescription",
          conflictingIndex
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else {
        SuccessResult(optionConverterNotifications)
      }
    }

  def doNothingIfExistsForLookup(
    entityType: EntityType,
    name: Option[Expression],
    options: Options,
    cypherVersion: CypherVersion
  ): (QueryContext, MapValue) => SchemaExecutionResult =
    (ctx, params) => {
      val indexName = getName(name, params)
      // Assert correct options to get errors even if matching index already exists
      val optionConverterNotifications = CreateLookupIndexOptionsConverter(ctx)
        .convert(cypherVersion, options, params, Some(ctx.getConfig))
        .toOptionNotification
        ._2

      val existingIndexDescriptor = Try(ctx.lookupIndexReference(entityType))
      if (existingIndexDescriptor.isSuccess) {
        // Notify on pre-existing index, replace potential parameter names with their actual value
        val indexDescription = lookupIndexInfo(indexName, entityType, options)
        val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(existingIndexDescriptor.get))

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $indexDescription",
          conflictingIndex
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else if (indexName.exists(ctx.indexExists)) {
        // Notify on pre-existing index, replace potential parameter names with their actual value
        val indexDescription = lookupIndexInfo(indexName, entityType, options)
        val conflictingIndex = existingIndexInfo(ctx, () => ctx.getIndexInformation(indexName.get))

        val notification = IndexOrConstraintAlreadyExistsNotification(
          s"CREATE $indexDescription",
          conflictingIndex
        )
        IgnoredResult(Set(notification) ++ optionConverterNotifications)
      } else {
        SuccessResult(optionConverterNotifications)
      }
    }

  // Help methods

  private def getMultipleEntityInfo(
    entityName: Either[List[LabelName], List[RelTypeName]],
    ctx: QueryContext
  ): (List[Int], EntityType) =
    entityName match {
      // returns (entityIds, EntityType)
      case Left(labels)    => (labels.map(label => ctx.getOrCreateLabelId(label.name)), EntityType.NODE)
      case Right(relTypes) => (relTypes.map(relType => ctx.getOrCreateRelTypeId(relType.name)), EntityType.RELATIONSHIP)
    }

  private def vectorIndexVersion(ctx: QueryContext): VectorIndexVersion =
    VectorIndexVersion.latestSupportedVersion(ctx.kernelVersion)

  private def indexInfo(
    indexType: String,
    nameOption: Option[String],
    entityName: ElementTypeName,
    properties: Seq[PropertyKeyName],
    options: Options
  ): String = {
    val name = getPrettyName(nameOption)
    val pattern = getPrettyEntityPattern(entityName)
    val propertyString = getPrettyPropertyPattern(properties, "(", ")")
    pretty"${asPrettyString.raw(indexType)} INDEX$name IF NOT EXISTS FOR $pattern ON $propertyString${prettyOptions(options)}".prettifiedString
  }

  private def fulltextIndexInfo(
    nameOption: Option[String],
    entityNames: Either[List[LabelName], List[RelTypeName]],
    properties: Seq[PropertyKeyName],
    options: Options
  ): String = {
    val name = getPrettyName(nameOption)
    val pattern = entityNames match {
      case Left(labels) =>
        val innerPattern = labels.map(l => asPrettyString(l.name)).mkPrettyString("e:", "|", "")
        pretty"($innerPattern)"
      case Right(relTypes) =>
        val innerPattern = relTypes.map(r => asPrettyString(r.name)).mkPrettyString("e:", "|", "")
        pretty"()-[$innerPattern]-()"
    }
    val propertyString = getPrettyPropertyPattern(properties, "[", "]")
    pretty"FULLTEXT INDEX$name IF NOT EXISTS FOR $pattern ON EACH $propertyString${prettyOptions(options)}".prettifiedString
  }

  private def vectorIndexInfo(
    nameOption: Option[String],
    entityNames: Either[List[LabelName], List[RelTypeName]],
    properties: Seq[PropertyKeyName],
    additionalProperties: Seq[PropertyKeyName],
    options: Options
  ): String = {
    val name = getPrettyName(nameOption)
    val pattern = entityNames match {
      case Left(labels) =>
        val innerPattern = labels.map(l => asPrettyString(l.name)).mkPrettyString("e:", "|", "")
        pretty"($innerPattern)"
      case Right(relTypes) =>
        val innerPattern = relTypes.map(r => asPrettyString(r.name)).mkPrettyString("e:", "|", "")
        pretty"()-[$innerPattern]-()"
    }
    val propertyString = getPrettyPropertyPattern(properties, "(", ")")
    val additionalPropertiesString =
      if (additionalProperties.nonEmpty) getPrettyPropertyPattern(additionalProperties, " WITH [", "]") else pretty""
    pretty"VECTOR INDEX$name IF NOT EXISTS FOR $pattern ON $propertyString$additionalPropertiesString${prettyOptions(options)}".prettifiedString
  }

  private def lookupIndexInfo(
    nameOption: Option[String],
    entityType: EntityType,
    options: Options
  ): String = {
    val name = getPrettyName(nameOption)
    val (pattern, function) = getPrettyLookupIndexPatternAndFunction(entityType == EntityType.NODE)
    pretty"LOOKUP INDEX$name IF NOT EXISTS FOR $pattern ON EACH $function${prettyOptions(options)}".prettifiedString
  }

  private def existingIndexInfo(
    ctx: QueryContext,
    getInfoParts: () => IndexInformation
  ): String = {
    try {
      // Assert we are allowed to see the index description
      ctx.assertShowIndexAllowed()

      // Fetch the relevant index parts
      val IndexInformation(isNode, indexType, name, entityNames, properties) = getInfoParts()

      // Create string description
      val nameString = getPrettyName(Some(name))
      val (pattern, on) = indexType match {
        case schema.IndexType.LOOKUP =>
          val (pattern, function) = getPrettyLookupIndexPatternAndFunction(isNode)
          (pattern, pretty"EACH $function")
        case schema.IndexType.FULLTEXT =>
          val innerPattern = entityNames.map(e => asPrettyString(e)).mkPrettyString("e:", "|", "")
          val pattern = if (isNode) pretty"($innerPattern)" else pretty"()-[$innerPattern]-()"
          val propertyString = getPrettyPropertyPattern(properties, "[", "]")
          (pattern, pretty"EACH $propertyString")
        case schema.IndexType.VECTOR =>
          val innerPattern = entityNames.map(e => asPrettyString(e)).mkPrettyString("e:", "|", "")
          val pattern = if (isNode) pretty"($innerPattern)" else pretty"()-[$innerPattern]-()"
          // Kernel only sees it as a single list with the vector property first
          val (vectorProperty, additionalProperties) = (properties.head, properties.tail)
          val propertyString = getPrettyPropertyPattern(List(vectorProperty), "(", ")")
          val additionalPropertiesString =
            if (additionalProperties.nonEmpty) getPrettyPropertyPattern(additionalProperties, " WITH [", "]")
            else pretty""
          (pattern, pretty"$propertyString$additionalPropertiesString")
        case _ =>
          // indexes have exactly one label/relType, unless FULLTEXT, LOOKUP or VECTOR which is handled above
          val pattern = getPrettyEntityPattern(isNode, entityNames.head)
          val propertyString = getPrettyPropertyPattern(properties, "(", ")")
          (pattern, propertyString)
      }
      pretty"${asPrettyString.raw(indexType.name())} INDEX$nameString FOR $pattern ON $on".prettifiedString
    } catch {
      // Not allowed to see index description, only show `index`
      case _: AuthorizationViolationException => "index"
    }
  }

  private def getPrettyLookupIndexPatternAndFunction(isNode: Boolean): (PrettyString, PrettyString) =
    if (isNode) {
      (pretty"(e)", pretty"${asPrettyString.raw(Labels.name)}(e)")
    } else {
      (pretty"()-[e]-()", pretty"${asPrettyString.raw(Type.name)}(e)")
    }
}
