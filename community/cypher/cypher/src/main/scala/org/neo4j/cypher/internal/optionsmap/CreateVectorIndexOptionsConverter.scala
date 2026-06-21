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
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.notification.VectorIndexDimensionsNotSpecifiedNotification
import org.neo4j.cypher.internal.runtime.IndexProviderContext
import org.neo4j.cypher.operations.CypherTypeValueMapper.valueType
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DIMENSIONS
import org.neo4j.internal.helpers.InclusiveRange
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue
import org.neo4j.internal.schema.IndexSettingRecord.State.INCORRECT_TYPE
import org.neo4j.internal.schema.IndexSettingRecord.State.INVALID_VALUE
import org.neo4j.internal.schema.IndexSettingRecord.State.MISSING_SETTING
import org.neo4j.internal.schema.IndexSettingRecord.State.UNRECOGNIZED_SETTING
import org.neo4j.internal.schema.IndexSettingRecordsByState
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.SettingsAccessor.MapValueAccessor
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.utils.PrettyPrinter.stringify
import org.neo4j.values.utils.ValueTypeNames.nameOfType
import org.neo4j.values.virtual.MapValue

import java.lang
import java.util.OptionalInt

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

case class CreateVectorIndexOptionsConverter(context: IndexProviderContext, latestSupportedVersion: VectorIndexVersion)
    extends IndexOptionsConverter[CreateIndexWithFullOptions] {
  private val schemaType = "vector index"

  override protected val hasMandatoryOptions: Boolean = true

  override def convert(
    options: MapValue,
    config: Option[Config],
    cypherVersion: CypherVersion
  ): OptionsConverterResult[CreateIndexWithFullOptions] = {
    val (indexProvider, indexConfig, notifications) =
      getOptionsParts(options, schemaType, IndexType.VECTOR, cypherVersion, getAlwaysUseLatestIndexProvider(config))
    val finalNotifications =
      if (indexConfig.get(VECTOR_DIMENSIONS.getSettingName) == null) {
        notifications + VectorIndexDimensionsNotSpecifiedNotification
      } else {
        notifications
      }
    ParsedWithNotifications(CreateIndexWithFullOptions(indexProvider, indexConfig), finalNotifications)
  }

  // VECTOR indexes have vector config settings
  override protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    maybeIndexProvider: Option[IndexProviderDescriptor]
  ): IndexConfig = {
    // current keys: vector.(dimensions|similarity_function|quantization.enabled|hnsw.m|hnsw.ef_construction)
    // current values: Long, String, Boolean

    def assertInvalidConfigValues(
      pp: PrettyPrinter,
      validationRecords: IndexSettingRecordsByState,
      itemsMap: MapValue,
      schemaType: String,
      validSettingNames: Iterable[String]
    ): Unit = {
      validationRecords.get(UNRECOGNIZED_SETTING).asScala.foreach {
        case fulltextSetting if validFulltextConfigSettingNames.contains(fulltextSetting.settingName) =>
          foundFulltextConfigValues(IndexType.VECTOR, pp, itemsMap, schemaType)
        case pointSetting if validPointConfigSettingNames.contains(pointSetting.settingName) =>
          foundPointConfigValues(IndexType.VECTOR, pp, itemsMap, schemaType)
        case unrecognized => throw InvalidArgumentsException.unrecognizedIndexConfigSetting(
            pp,
            itemsMap,
            unrecognized,
            schemaType,
            validSettingNames.toSeq.asJava
          )
      }
    }

    def assertMandatoryConfigSettingsExists(validationRecords: IndexSettingRecordsByState): Unit = {
      val missingSettings = validationRecords.get(MISSING_SETTING).asScala
      if (missingSettings.nonEmpty) {
        val missingQuoted =
          missingSettings.map(r => s"'${r.settingName}'").mkString(
            "[",
            ", ",
            "]"
          )
        val missing = missingSettings.map(_.settingName).toSeq.asJava
        throw InvalidArgumentsException.missingOptionCreateSchema(schemaType, missing, missingQuoted)
      }
    }

    def assertConfigSettingsCorrectTypes(validationRecords: IndexSettingRecordsByState, itemsMap: MapValue): Unit = {
      val legacyExceptionValidTypes: Map[Class[_], String] =
        Map(
          classOf[IntegralValue] -> "an Integer",
          classOf[FloatingPointValue] -> "a Float",
          classOf[TextValue] -> "a String",
          classOf[BooleanValue] -> "a Boolean"
        )

      validationRecords.get(INCORRECT_TYPE).asScala.foreach {
        // valid type for vector index config, *but* invalid for that setting
        case incorrectType: IncorrectType if legacyExceptionValidTypes.exists { case (cls, _) =>
            cls.isAssignableFrom(incorrectType.providedType)
          } =>
          throw InvalidArgumentsException.invalidVectorIndexConfigSetting(
            schemaType,
            incorrectType.settingName,
            stringify(incorrectType.value),
            legacyExceptionValidTypes(incorrectType.targetType),
            nameOfType(incorrectType.targetType)
          )
        // invalid type for valid type for vector index config
        case _ => throw InvalidArgumentsException.invalidVectorIndexConfig(schemaType, itemsMap)
      }
    }

    def assertValidConfigValues(validationRecords: IndexSettingRecordsByState): Unit = {
      validationRecords.get(INVALID_VALUE).asScala.map(_.asInstanceOf[InvalidValue]).foreach {
        invalidValue =>
          val value = invalidValue.value match {
            case maybeInt: OptionalInt if maybeInt.isEmpty => Values.NO_VALUE
            case maybeInt: OptionalInt                     => maybeInt.getAsInt
            case value                                     => value
          }
          val requirement = invalidValue.requirement
          requirement.get match {
            case range: InclusiveRange[_] => throw InvalidArgumentsException.indexSettingOutOfRange(
                invalidValue.settingName,
                valueType(Values.of(range.min)),
                range.min.toString,
                range.max.toString,
                value
              )
            case iterable: lang.Iterable[_] => throw InvalidArgumentsException.invalidIndexSettingValue(
                invalidValue.settingName,
                iterable.asScala.map(_.toString).toList.asJava,
                value
              )
            case _ =>
              val valueString = stringify(value)
              val settingName = invalidValue.settingName
              InvalidArgumentException.invalidIndexInput(
                valueString,
                settingName,
                "'%s' is an unsupported '%s'. Supported: %s"
                  .formatted(stringify(value), settingName, requirement.supported())
              )
          }
      }
    }

    config match {
      case itemsMap: MapValue =>
        val version = maybeIndexProvider.map(VectorIndexVersion.fromDescriptor).getOrElse(latestSupportedVersion)
        val validator = version.indexSettingValidator
        val validationRecords = validator.validate(new MapValueAccessor(itemsMap))
        if (validationRecords.valid) return validator.validateToTypedConfig(validationRecords).config

        assertInvalidConfigValues(
          new PrettyPrinter(),
          validationRecords,
          itemsMap,
          schemaType,
          validator.acceptedSettings.asScala.map(_.getSettingName)
        )
        assertMandatoryConfigSettingsExists(validationRecords)
        assertConfigSettingsCorrectTypes(validationRecords, itemsMap)
        assertValidConfigValues(validationRecords)
        validator.validateToTypedConfig(validationRecords).config
      case unknown =>
        throw InvalidArgumentsException.invalidVectorIndexConfig(schemaType, unknown)
    }
  }

  override def operation: String = s"create $schemaType"
}
