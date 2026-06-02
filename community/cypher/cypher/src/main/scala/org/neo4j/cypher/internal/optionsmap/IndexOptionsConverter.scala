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
package org.neo4j.cypher.internal.optionsmap

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.cypher.internal.notification.DeprecatedIndexProviderOption
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.runtime.IndexProviderContext
import org.neo4j.graphdb.schema.IndexSetting
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_ANALYZER
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DEFAULT_SEARCH_EXPANSION_FACTOR
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DIMENSIONS
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_HNSW_EF_CONSTRUCTION
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_HNSW_M
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_QUANTIZATION_ENABLED
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_QUANTIZATION_TYPE
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_SIMILARITY_FUNCTION
import org.neo4j.graphdb.schema.IndexSettingUtil
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.lang.String.CASE_INSENSITIVE_ORDER

import scala.collection.immutable.SortedSet
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.math.Ordering.comparatorToOrdering

trait IndexOptionsConverter[T] extends OptionsConverter[T] {
  protected def context: IndexProviderContext

  protected def getAlwaysUseLatestIndexProvider(config: Option[Config]): Boolean = {
    config match {
      case Some(cfg) => Boolean unbox cfg.get(GraphDatabaseInternalSettings.always_use_latest_index_provider)
      case None      => true
    }
  }

  protected def getOptionsParts(
    options: MapValue,
    schemaType: String,
    indexType: IndexType,
    version: CypherVersion,
    alwaysUseLatestIndexProvider: Boolean
  ): (Option[IndexProviderDescriptor], IndexConfig, Set[InternalNotification]) = {

    val (validOptions, errorMessageOverride) =
      if (version == CypherVersion.Cypher5)
        (
          Seq("indexProvider", "indexConfig"),
          Some(
            s"Failed to create $schemaType: Invalid option provided, valid options are `indexProvider` and `indexConfig`."
          )
        )
      else (Seq("indexConfig"), None)

    val invalidOption: Option[String] = options.collectFirst {
      case (k, _) if !validOptions.exists(_.equalsIgnoreCase(k)) => k
    }

    invalidOption.foreach(k =>
      throw InvalidArgumentsException.invalidIndexOptionValue(
        k,
        validOptions.asJava,
        errorMessageOverride.orNull
      )
    )

    // User provided index provider is deprecated in Cypher 5, and removed in Cypher 25.
    // Should use the actual index provider for index configuration validation.
    // By default the latest index provider is used, but is configurable with internal setting.
    val maybeIndexProvider = options.getOption("indexprovider")
    val deprecatedUserProvidedIndexProvider =
      maybeIndexProvider.map(assertValidIndexProvider(_, schemaType, indexType, version))
    val indexProvider: Option[IndexProviderDescriptor] = deprecatedUserProvidedIndexProvider match {
      case Some(_) if alwaysUseLatestIndexProvider => None
      case any                                     => any
    }

    // If there are mandatory options we should call convert with empty options to throw expected errors
    val maybeConfig = options.getOption("indexconfig").orElse(Option.when(hasMandatoryOptions)(VirtualValues.EMPTY_MAP))
    val indexConfig =
      maybeConfig.map(assertValidAndTransformConfig(_, schemaType, indexProvider)).getOrElse(IndexConfig.empty)

    val notifications: Set[InternalNotification] =
      if (deprecatedUserProvidedIndexProvider.nonEmpty) Set(DeprecatedIndexProviderOption()) else Set.empty

    (indexProvider, indexConfig, notifications)
  }

  protected def toIndexConfig: java.util.Map[String, Object] => IndexConfig =
    IndexSettingUtil.toIndexConfigFromStringObjectMap

  protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): IndexConfig

  private def assertValidIndexProvider(
    indexProvider: AnyValue,
    schemaType: String,
    indexType: IndexType,
    version: CypherVersion
  ): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      context.validateIndexProvider(schemaType, indexProviderValue.stringValue(), indexType, version)
    case _ => throw InvalidArgumentsException.invalidIndexProvider(schemaType, indexProvider)
  }

  protected val validPointConfigSettingNames: SortedSet[String] = indexSettingsToCaseInsensitiveNames(
    SPATIAL_CARTESIAN_MIN,
    SPATIAL_CARTESIAN_MAX,
    SPATIAL_CARTESIAN_3D_MIN,
    SPATIAL_CARTESIAN_3D_MAX,
    SPATIAL_WGS84_MIN,
    SPATIAL_WGS84_MAX,
    SPATIAL_WGS84_3D_MIN,
    SPATIAL_WGS84_3D_MAX
  )

  private def getValidConfigNames(idxType: IndexType): java.util.List[String] = {
    idxType match {
      case IndexType.FULLTEXT => validFulltextConfigSettingNames.toList.asJava
      case IndexType.VECTOR   => validVectorConfigSettingNames.toList.asJava
      case IndexType.POINT    => validPointConfigSettingNames.toList.asJava
      // No other index types have valid settings,
      // but we get here if you give fulltext, vector or point in their configs
      case _ => java.util.List.of()
    }
  }

  protected def checkForPointConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit =
    if (itemsMap.exists { case (p, _) => validPointConfigSettingNames.contains(p) }) {
      foundPointConfigValues(originIndexType, pp, itemsMap, schemaType)
    }

  protected def foundPointConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit = {
    throw InvalidArgumentsException.pointOptionsInConfig(pp, itemsMap, schemaType, getValidConfigNames(originIndexType))
  }

  protected val validFulltextConfigSettingNames: SortedSet[String] =
    indexSettingsToCaseInsensitiveNames(FULLTEXT_ANALYZER, FULLTEXT_EVENTUALLY_CONSISTENT)

  protected def checkForFulltextConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit =
    if (itemsMap.exists { case (p, _) => validFulltextConfigSettingNames.contains(p) }) {
      foundFulltextConfigValues(originIndexType, pp, itemsMap, schemaType)
    }

  protected def foundFulltextConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit = {
    val prettyVal = new PrettyPrinter()
    itemsMap.writeTo(prettyVal)
    throw InvalidArgumentsException.fulltextOptionsInConfig(
      pp,
      itemsMap,
      schemaType,
      getValidConfigNames(originIndexType)
    )
  }

  private val validVectorConfigSettingNames: SortedSet[String] =
    indexSettingsToCaseInsensitiveNames(
      VECTOR_DIMENSIONS,
      VECTOR_SIMILARITY_FUNCTION,
      VECTOR_DEFAULT_SEARCH_EXPANSION_FACTOR,
      VECTOR_QUANTIZATION_ENABLED,
      VECTOR_QUANTIZATION_TYPE,
      VECTOR_HNSW_M,
      VECTOR_HNSW_EF_CONSTRUCTION
    )

  protected def checkForVectorConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit =
    if (itemsMap.exists { case (p, _) => validVectorConfigSettingNames.contains(p) }) {

      foundVectorConfigValues(originIndexType, pp, itemsMap, schemaType)
    }

  private def foundVectorConfigValues(
    originIndexType: IndexType,
    pp: PrettyPrinter,
    itemsMap: MapValue,
    schemaType: String
  ): Unit = {
    throw InvalidArgumentsException.vectorOptionsInConfig(
      pp,
      itemsMap,
      schemaType,
      getValidConfigNames(originIndexType)
    )
  }

  protected def assertEmptyConfig(
    config: AnyValue,
    schemaType: String,
    indexType: IndexType
  ): IndexConfig = {
    // no available config settings, throw nice error when existing config settings for other index types

    val indexString = indexType match {
      case IndexType.TEXT   => "text"
      case IndexType.LOOKUP => "lookup"
      case IndexType.RANGE  => "range"
      case _                => indexType.toString.toLowerCase
    }

    val pp = new PrettyPrinter()
    config match {
      case itemsMap: MapValue if !itemsMap.isEmpty =>
        checkForFulltextConfigValues(
          null,
          pp,
          itemsMap,
          schemaType
        )
        checkForPointConfigValues(indexType, pp, itemsMap, schemaType)
        checkForVectorConfigValues(indexType, pp, itemsMap, schemaType)

        itemsMap.writeTo(pp)
        throw InvalidArgumentsException.invalidIndexConfig(schemaType, pp.value(), indexString)
      case _: MapValue => IndexConfig.empty
      case unknown     => throw InvalidArgumentsException.invalidIndexConfigExpectedMap(schemaType, unknown)
    }
  }

  private def indexSettingsToCaseInsensitiveNames(settings: IndexSetting*): SortedSet[String] =
    SortedSet.from(settings.iterator.map(_.getSettingName))(comparatorToOrdering(CASE_INSENSITIVE_ORDER))
}

case class CreateIndexProviderOnlyOptions(provider: Option[IndexProviderDescriptor])

case class CreateIndexWithFullOptions(provider: Option[IndexProviderDescriptor], config: IndexConfig)

case class CreateWithNoOptions()
