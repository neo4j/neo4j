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
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DIMENSIONS
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_SIMILARITY_FUNCTION
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.KernelVersion
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion
import org.neo4j.util.Preconditions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.MapValue

import java.util.Locale
import java.util.Objects

import scala.jdk.CollectionConverters.IterableHasAsScala

case class CreateVectorIndexOptionsConverter(context: QueryContext)
    extends IndexOptionsConverter[CreateIndexWithFullOptions] {
  private val schemaType = "vector index"
  private val dimensionsSetting = VECTOR_DIMENSIONS.getSettingName
  private val similarityFunctionSetting = VECTOR_SIMILARITY_FUNCTION.getSettingName

  override protected val hasMandatoryOptions: Boolean = true

  override def convert(options: MapValue, config: Option[Config]): CreateIndexWithFullOptions = {
    val (indexProvider, indexConfig) = getOptionsParts(options, schemaType, IndexType.VECTOR)
    CreateIndexWithFullOptions(indexProvider, indexConfig)
  }

  // VECTOR indexes has vector config settings
  override protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): IndexConfig = {
    // current keys: vector.(dimensions|similarity_function)
    // current values: Long, String

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(
        s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map from String to Strings and Integers."
      )
    }

    config match {
      case itemsMap: MapValue if itemsMap.isEmpty =>
        assertMandatoryConfigSettingsExists(Set.empty)
        IndexConfig.empty // should not reach here
      case itemsMap: MapValue =>
        checkForFulltextConfigValues(new PrettyPrinter(), itemsMap, schemaType)
        checkForPointConfigValues(new PrettyPrinter(), itemsMap, schemaType)

        // throw error early on missing config settings
        assertMandatoryConfigSettingsExists(itemsMap.keySet().asScala.toSet)

        val hm = new java.util.HashMap[String, Object]()
        itemsMap.foreach {
          case (p: String, e: TextValue) =>
            hm.put(p, e.stringValue().toUpperCase(Locale.ROOT))
          case (p: String, e: IntValue) =>
            hm.put(p, java.lang.Integer.valueOf(e.intValue()))
          case (p: String, e: LongValue) =>
            hm.put(p, java.lang.Long.valueOf(e.longValue()))
          case _ => throw exceptionWrongType(itemsMap)
        }

        // Need to validate config settings here in the same way that is done in the procedure
        // since the exceptions given on procedure level is nicer than the ones down in kernel
        assertValidConfigValues(
          indexProvider,
          hm.get(VECTOR_DIMENSIONS.getSettingName),
          hm.get(VECTOR_SIMILARITY_FUNCTION.getSettingName)
        )

        toIndexConfig(hm)
      case unknown =>
        throw exceptionWrongType(unknown)
    }
  }

  private def assertMandatoryConfigSettingsExists(givenConfigSettings: Set[String]): Unit = {
    val hasDimensions =
      givenConfigSettings.map(_.toLowerCase(Locale.ROOT)).contains(dimensionsSetting.toLowerCase(Locale.ROOT))
    val hasSimilarityFunction =
      givenConfigSettings.map(_.toLowerCase(Locale.ROOT)).contains(similarityFunctionSetting.toLowerCase(Locale.ROOT))

    val missingConfig =
      if (!hasDimensions && !hasSimilarityFunction) {
        Some(List(dimensionsSetting, similarityFunctionSetting).sorted.mkString("'", "', '", "'"))
      } else if (!hasDimensions) {
        Some(s"'$dimensionsSetting'")
      } else if (!hasSimilarityFunction) {
        Some(s"'$similarityFunctionSetting'")
      } else {
        None
      }
    if (missingConfig.nonEmpty) {
      throw new InvalidArgumentsException(
        s"Failed to create $schemaType: Missing index config options [${missingConfig.get}]."
      )
    }
  }

  // Do the same checks as in the procedure + check for correct type
  // The checks would still be done and errors thrown otherwise but they'd be wrapped in less helpful errors,
  // so only looking at the top error would not give you the reason for the failure
  private def assertValidConfigValues(
    maybeIndexProvider: Option[IndexProviderDescriptor],
    dimensionValue: AnyRef,
    similarityFunctionValue: AnyRef
  ): Unit = {
    val version = maybeIndexProvider.map(VectorIndexVersion.fromDescriptor).getOrElse(
      VectorIndexVersion.latestSupportedVersion(KernelVersion.getLatestVersion(context.getConfig))
    )

    // Check dimension
    val maxDimensions = version.maxDimensions
    Objects.requireNonNull(dimensionValue, s"'$dimensionsSetting' must not be null")
    val vectorDimensionCheck = dimensionValue match {
      case l: java.lang.Long =>
        val vectorDimension = l.longValue()
        1 <= vectorDimension && vectorDimension <= maxDimensions
      case i: Integer =>
        val vectorDimension = i.intValue()
        1 <= vectorDimension && vectorDimension <= maxDimensions
      case _ =>
        throw new InvalidArgumentsException(
          s"Could not create $schemaType with specified index config '$dimensionsSetting'. Expected an Integer."
        )
    }
    Preconditions.checkArgument(
      vectorDimensionCheck,
      "'%s' must be between %d and %d inclusively".formatted(dimensionsSetting, 1, maxDimensions)
    )

    // Check similarity function
    Objects.requireNonNull(similarityFunctionValue, s"'$similarityFunctionSetting' must not be null")
    similarityFunctionValue match {
      case s: String =>
        version.similarityFunction(s)
      case _ =>
        throw new InvalidArgumentsException(
          s"Could not create $schemaType with specified index config '$similarityFunctionSetting'. Expected a String."
        )
    }
  }

  override def operation: String = s"create $schemaType"
}
