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

import Ordering.comparatorToOrdering
import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.cypher.internal.runtime.QueryContext
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
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DIMENSIONS
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_HNSW_EF_CONSTRUCTION
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_HNSW_M
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_QUANTIZATION_ENABLED
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

trait IndexOptionsConverter[T] extends OptionsConverter[T] {
  protected def context: QueryContext

  protected def getOptionsParts(
    options: MapValue,
    schemaType: String,
    indexType: IndexType
  ): (Option[IndexProviderDescriptor], IndexConfig) = {

    if (options.exists { case (k, _) => !k.equalsIgnoreCase("indexProvider") && !k.equalsIgnoreCase("indexConfig") }) {
      throw new InvalidArgumentsException(
        s"Failed to create $schemaType: Invalid option provided, valid options are `indexProvider` and `indexConfig`."
      )
    }
    val maybeIndexProvider = options.getOption("indexprovider")
    // If there are mandatory options we should call convert with empty options to throw expected errors
    val maybeConfig = options.getOption("indexconfig").orElse(Option.when(hasMandatoryOptions)(VirtualValues.EMPTY_MAP))

    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider(_, schemaType, indexType))
    val indexConfig =
      maybeConfig.map(assertValidAndTransformConfig(_, schemaType, indexProvider)).getOrElse(IndexConfig.empty)

    (indexProvider, indexConfig)
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
    indexType: IndexType
  ): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      context.validateIndexProvider(schemaType, indexProviderValue.stringValue(), indexType)
    case _ =>
      throw new InvalidArgumentsException(
        s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value."
      )
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

  protected def checkForPointConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (itemsMap.exists { case (p, _) => validPointConfigSettingNames.contains(p) }) {
      foundPointConfigValues(pp, itemsMap, schemaType)
    }

  protected def foundPointConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit = {
    throw new InvalidArgumentsException(
      s"""${invalidConfigValueString(pp, itemsMap, schemaType)}, contains spatial config settings options.
         |To create point index, please use 'CREATE POINT INDEX ...'.""".stripMargin
    )
  }

  protected val validFulltextConfigSettingNames: SortedSet[String] =
    indexSettingsToCaseInsensitiveNames(FULLTEXT_ANALYZER, FULLTEXT_EVENTUALLY_CONSISTENT)

  protected def checkForFulltextConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (itemsMap.exists { case (p, _) => validFulltextConfigSettingNames.contains(p) }) {
      foundFulltextConfigValues(pp, itemsMap, schemaType)
    }

  protected def foundFulltextConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit = {
    throw new InvalidArgumentsException(
      s"""${invalidConfigValueString(pp, itemsMap, schemaType)}, contains fulltext config options.
         |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin
    )
  }

  private val validVectorConfigSettingNames: SortedSet[String] =
    indexSettingsToCaseInsensitiveNames(
      VECTOR_DIMENSIONS,
      VECTOR_SIMILARITY_FUNCTION,
      VECTOR_QUANTIZATION_ENABLED,
      VECTOR_HNSW_M,
      VECTOR_HNSW_EF_CONSTRUCTION
    )

  protected def checkForVectorConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (itemsMap.exists { case (p, _) => validVectorConfigSettingNames.contains(p) }) {
      foundVectorConfigValues(pp, itemsMap, schemaType)
    }

  private def foundVectorConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit = {
    throw new InvalidArgumentsException(
      s"""${invalidConfigValueString(pp, itemsMap, schemaType)}, contains vector config options.
         |To create vector index, please use 'CREATE VECTOR INDEX ...'.""".stripMargin
    )
  }

  protected def invalidConfigValueString(pp: PrettyPrinter, value: AnyValue, schemaType: String): String = {
    value.writeTo(pp)
    invalidConfigValueString(pp.value(), schemaType)
  }

  protected def invalidConfigValueString(value: String, schemaType: String): String =
    s"Could not create $schemaType with specified index config '$value'"

  protected def assertEmptyConfig(
    config: AnyValue,
    schemaType: String,
    indexType: String
  ): IndexConfig = {
    // no available config settings, throw nice error when existing config settings for other index types
    val pp = new PrettyPrinter()
    config match {
      case itemsMap: MapValue if !itemsMap.isEmpty =>
        checkForFulltextConfigValues(pp, itemsMap, schemaType)
        checkForPointConfigValues(pp, itemsMap, schemaType)
        checkForVectorConfigValues(pp, itemsMap, schemaType)

        itemsMap.writeTo(pp)
        throw new InvalidArgumentsException(
          s"""Could not create $schemaType with specified index config '${pp.value()}': $indexType indexes have no valid config values.""".stripMargin
        )
      case _: MapValue => IndexConfig.empty
      case unknown =>
        unknown.writeTo(pp)
        throw new InvalidArgumentsException(
          s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map."
        )
    }
  }

  private def indexSettingsToCaseInsensitiveNames(settings: IndexSetting*): SortedSet[String] =
    SortedSet.from(settings.iterator.map(_.getSettingName))(comparatorToOrdering(CASE_INSENSITIVE_ORDER))
}

case class CreateIndexProviderOnlyOptions(provider: Option[IndexProviderDescriptor])

case class CreateIndexWithFullOptions(provider: Option[IndexProviderDescriptor], config: IndexConfig)

case class CreateWithNoOptions()
