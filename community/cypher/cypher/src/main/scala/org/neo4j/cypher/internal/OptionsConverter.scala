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
package org.neo4j.cypher.internal

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.evaluator.Evaluator
import org.neo4j.cypher.internal.evaluator.ExpressionEvaluator
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
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
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_SIMILARITY_FUNCTION
import org.neo4j.graphdb.schema.IndexSettingUtil
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.KernelVersion
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.StorageEngineFactory.allAvailableStorageEngines
import org.neo4j.util.Preconditions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues

import java.lang.Boolean.FALSE
import java.util.Collections
import java.util.Locale
import java.util.Objects
import java.util.UUID

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

trait OptionsConverter[T] {

  val evaluator: ExpressionEvaluator = Evaluator.expressionEvaluator()

  def evaluate(expression: Expression, params: MapValue): AnyValue = {
    evaluator.evaluate(expression, params)
  }

  def convert(options: Options, params: MapValue, config: Option[Config] = None): Option[T] = options match {
    case NoOptions if hasMandatoryOptions =>
      // If there are mandatory options we should call convert with empty options to throw expected errors
      Some(convert(VirtualValues.EMPTY_MAP, config))
    case NoOptions => None
    case OptionsMap(map) => Some(convert(
        VirtualValues.map(
          map.keys.map(_.toLowerCase(Locale.ROOT)).toArray,
          map.view.mapValues(evaluate(_, params)).values.toArray
        ),
        config
      ))
    case OptionsParam(parameter) =>
      val opsMap = params.get(parameter.name)
      opsMap match {
        case mv: MapValue =>
          val builder = new MapValueBuilder()
          mv.foreach((k, v) => builder.add(k.toLowerCase(Locale.ROOT), v))
          Some(convert(builder.build(), config))
        case _ =>
          throw new InvalidArgumentsException(s"Could not $operation with options '$opsMap'. Expected a map value.")
      }
  }

  implicit def operation: String

  def convert(options: MapValue, config: Option[Config]): T

  protected val hasMandatoryOptions: Boolean = false
}

case object ServerOptionsConverter extends OptionsConverter[ServerOptions] {
  private val ALLOWED_DATABASES = "allowedDatabases"
  private val DENIED_DATABASES = "deniedDatabases"
  private val MODE_CONSTRAINT = "modeConstraint"
  private val TAGS = "tags"

  val VISIBLE_PERMITTED_OPTIONS = s"'$ALLOWED_DATABASES', '$DENIED_DATABASES', '$MODE_CONSTRAINT'"

  override def operation: String = "enable server"

  override def convert(map: MapValue, config: Option[Config]): ServerOptions = {
    map.foldLeft(ServerOptions(None, None, None, None)) {
      case (ops, (key, value)) =>
        if (key.equalsIgnoreCase(ALLOWED_DATABASES)) {
          value match {
            case list: ListValue =>
              val databases: Set[NormalizedDatabaseName] = list.iterator().asScala.map {
                case t: TextValue => new NormalizedDatabaseName(t.stringValue())
                case _ => throw new InvalidArgumentsException(
                    s"$ALLOWED_DATABASES expects a list of database names but got '$list'."
                  )
              }.toSet
              ops.copy(allowed = Some(databases))
            case t: TextValue =>
              ops.copy(allowed = Some(Set(new NormalizedDatabaseName(t.stringValue()))))
            case value: AnyValue =>
              throw new InvalidArgumentsException(
                s"$ALLOWED_DATABASES expects a list of database names but got '$value'."
              )
          }
        } else if (key.equalsIgnoreCase(DENIED_DATABASES)) {
          value match {
            case list: ListValue =>
              val databases: Set[NormalizedDatabaseName] = list.iterator().asScala.map {
                case t: TextValue => new NormalizedDatabaseName(t.stringValue())
                case _ => throw new InvalidArgumentsException(
                    s"$DENIED_DATABASES expects a list of database names but got '$list'."
                  )
              }.toSet
              ops.copy(denied = Some(databases))
            case t: TextValue =>
              ops.copy(denied = Some(Set(new NormalizedDatabaseName(t.stringValue()))))
            case value: AnyValue =>
              throw new InvalidArgumentsException(
                s"$DENIED_DATABASES expects a list of database names but got '$value'."
              )
          }
        } else if (key.equalsIgnoreCase(MODE_CONSTRAINT)) {
          value match {
            case t: TextValue =>
              val mode =
                try {
                  InstanceModeConstraint.valueOf(t.stringValue().toUpperCase(Locale.ROOT))
                } catch {
                  case _: Exception =>
                    throw new InvalidArgumentsException(
                      s"$MODE_CONSTRAINT expects 'NONE', 'PRIMARY' or 'SECONDARY' but got '$value'."
                    )
                }
              ops.copy(mode = Some(mode))
            case value: AnyValue =>
              throw new InvalidArgumentsException(
                s"$MODE_CONSTRAINT expects 'NONE', 'PRIMARY' or 'SECONDARY' but got '$value'."
              )
          }
        } else if (key.equalsIgnoreCase(TAGS)) {
          value match {
            case list: ListValue =>
              val tags: List[String] = list.iterator().asScala.map {
                case t: TextValue => t.stringValue()
                case _ => throw new InvalidArgumentsException(
                    s"$TAGS expects a list of tags but got '$list'."
                  )
              }.toList
              ops.copy(tags = Some(tags))
            case t: TextValue =>
              ops.copy(tags = Some(List(t.stringValue())))
            case value: AnyValue =>
              throw new InvalidArgumentsException(
                s"$TAGS expects a list of tags names but got '$value'."
              )
          }
        } else {
          throw new InvalidArgumentsException(
            s"Unrecognised option '$key', expected $VISIBLE_PERMITTED_OPTIONS."
          )
        }
    }
  }
}

trait OptionValidator[T] {

  val KEY: String

  protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): T

  def findIn(optionsMap: MapValue, config: Option[Config])(implicit operation: String): Option[T] = {
    optionsMap
      .find(_._1.equalsIgnoreCase(KEY))
      .map(_._2)
      .flatMap {
        case _: NoValue => None
        case value      => Some(value)
      }
      .map(validate(_, config))
  }
}

trait StringOptionValidator extends OptionValidator[String] {

  protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit

  override protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): String = {
    value match {
      case textValue: TextValue =>
        validateContent(textValue.stringValue(), config)
        textValue.stringValue()
      case _ =>
        throw new InvalidArgumentsException(s"Could not $operation with specified $KEY '$value', String expected.")
    }
  }
}

object ExistingDataOption extends StringOptionValidator {
  val KEY = "existingData"

  // possible options:
  val VALID_VALUE = "use"

  // override to keep legacy behaviour. ExistingDataOption is parsed to lowercase, other options keep input casing.
  override protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): String =
    super.validate(value, config).toLowerCase(Locale.ROOT)

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    if (!value.equalsIgnoreCase(VALID_VALUE)) {
      throw new InvalidArgumentsException(
        s"Could not $operation with specified $KEY '$value'. Expected '$VALID_VALUE'."
      )
    }
  }
}

object ExistingSeedInstanceOption extends StringOptionValidator {
  override val KEY: String = "existingDataSeedInstance"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit =
    try {
      UUID.fromString(value)
    } catch {
      case _: IllegalArgumentException =>
        throw new InvalidArgumentsException(
          s"Could not $operation with specified $KEY '$value'. Expected server uuid string."
        )
    }
}

object StoreFormatOption extends StringOptionValidator {
  override val KEY: String = "storeFormat"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    try {
      // Validate the format by looking for a storage engine that supports it - will throw if none was found
      val versionsUnderDev =
        config.fold(FALSE)(_.get(GraphDatabaseInternalSettings.include_versions_under_development))
      val selectEngineConfig = Config.newBuilder()
        .set(GraphDatabaseSettings.db_format, value)
        .set(GraphDatabaseInternalSettings.include_versions_under_development, versionsUnderDev)
        .build()
      StorageEngineFactory.selectStorageEngine(selectEngineConfig)
    } catch {
      case _: Exception =>
        val allFormats = allAvailableStorageEngines().asScala
          .flatMap(sef => sef.supportedFormats(false).asScala.toSeq.sorted)
          .toSeq.distinct
          .mkString("'", "', '", "'")
        throw new InvalidArgumentsException(
          s"Could not $operation with specified $KEY '$value'. Unknown format, supported formats are " + allFormats
        )
    }
  }
}

object SeedURIOption extends StringOptionValidator {
  override val KEY: String = "seedURI"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    // no content validation, any string is accepted
  }
}

object SeedCredentialsOption extends StringOptionValidator {
  override val KEY: String = "seedCredentials"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    // no content validation, any string is accepted
  }
}

object SeedConfigOption extends StringOptionValidator {
  override val KEY: String = "seedConfig"

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    // no content validation, any string is accepted
  }
}

object LogEnrichmentOption extends StringOptionValidator {
  override val KEY: String = "txLogEnrichment"

  private val FULL: String = "FULL"
  private val DIFF: String = "DIFF"
  private val OFF: String = "OFF"
  private val validValues = Seq(FULL, DIFF, OFF)

  // override to normalize to uppercase.
  override protected def validate(value: AnyValue, config: Option[Config])(implicit operation: String): String =
    super.validate(value, config).toUpperCase(Locale.ROOT)

  override protected def validateContent(value: String, config: Option[Config])(implicit operation: String): Unit = {
    if (!validValues.exists(value.equalsIgnoreCase)) {
      throw new InvalidArgumentsException(
        s"Could not $operation with specified $KEY '$value', Expected one of ${validValues.mkString("'", "', '", "'")}"
      )
    }
  }
}

case object AlterDatabaseOptionsConverter extends OptionsConverter[AlterDatabaseOptions] {

  // expectedKeys must be kept in sync with AlterDatabaseOptions below!
  private val expectedKeys: Map[String, String] = Map(
    LogEnrichmentOption.KEY.toLowerCase(Locale.ROOT) -> LogEnrichmentOption.KEY
  )

  private val VISIBLE_PERMITTED_OPTIONS: String = expectedKeys.values.map(opt => s"'$opt'").mkString(", ")

  def validForRemoval(keys: Set[String], config: Config): Set[String] = {
    if (keys.nonEmpty && !config.get(GraphDatabaseInternalSettings.change_data_capture)) {
      throw new UnsupportedOperationException("Removing options is not supported yet")
    }
    val (validKeys, invalidKeys) = keys.partition(key => expectedKeys.contains(key.toLowerCase(Locale.ROOT)))
    if (invalidKeys.nonEmpty) throwErrorForInvalidKeys(invalidKeys, s"$operation remove")
    validKeys.map(key => expectedKeys(key.toLowerCase(Locale.ROOT)))
  }

  override def convert(optionsMap: MapValue, config: Option[Config]): AlterDatabaseOptions = {
    if (optionsMap.nonEmpty && !config.exists(_.get(GraphDatabaseInternalSettings.change_data_capture))) {
      throw new UnsupportedOperationException("Setting options in alter is not supported yet")
    }
    val invalidKeys = optionsMap.keySet().asScala.toSeq.filterNot(found =>
      expectedKeys.contains(found.toLowerCase(Locale.ROOT))
    )
    if (invalidKeys.nonEmpty) throwErrorForInvalidKeys(invalidKeys, operation)

    // Keys must be kept in sync with expectedKeys above!
    AlterDatabaseOptions(
      txLogEnrichment = LogEnrichmentOption.findIn(optionsMap, config)
    )
  }

  private def throwErrorForInvalidKeys(invalidKeys: Iterable[String], operation: String) = {
    val validForCreateDatabase =
      invalidKeys.filter(invalidKey =>
        CreateDatabaseOptionsConverter.expectedKeys.map(_.toLowerCase(Locale.ROOT)).contains(
          invalidKey.toLowerCase(Locale.ROOT)
        )
      )

    if (validForCreateDatabase.isEmpty) {
      // keys are not even valid for CREATE DATABASE OPTIONS
      throw new InvalidArgumentsException(
        s"Could not $operation with unrecognised option(s): ${invalidKeys.mkString("'", "', '", "'")}. Expected $VISIBLE_PERMITTED_OPTIONS."
      )
    } else {
      // keys are valid in CREATE DATABASE OPTIONS, but not allowed to be mutated through ALTER DATABASE SET OPTION
      throw new InvalidArgumentsException(
        s"Could not $operation with 'CREATE DATABASE' option(s): ${validForCreateDatabase.mkString("'", "', '", "'")}. Expected $VISIBLE_PERMITTED_OPTIONS."
      )
    }
  }

  implicit override def operation: String = "alter database"

}

case object CreateDatabaseOptionsConverter extends OptionsConverter[CreateDatabaseOptions] {

  // expectedKeys must be kept in sync with CreateDatabaseOptions below!
  val expectedKeys: Set[String] = Set(
    ExistingDataOption.KEY,
    ExistingSeedInstanceOption.KEY,
    StoreFormatOption.KEY,
    SeedURIOption.KEY,
    SeedCredentialsOption.KEY,
    SeedConfigOption.KEY,
    LogEnrichmentOption.KEY
  )

  val VISIBLE_PERMITTED_OPTIONS: String = expectedKeys.map(opt => s"'$opt'").mkString(", ")

  override def convert(optionsMap: MapValue, config: Option[Config]): CreateDatabaseOptions = {
    if (
      optionsMap.keySet().asScala.map(_.toLowerCase(Locale.ROOT)).toSeq.contains(
        LogEnrichmentOption.KEY.toLowerCase(Locale.ROOT)
      ) &&
      !config.exists(_.get(GraphDatabaseInternalSettings.change_data_capture))
    ) {
      throw new UnsupportedOperationException(s"${LogEnrichmentOption.KEY} is not supported yet")
    }
    val invalidKeys = optionsMap.keySet().asScala.toSeq.filterNot(found =>
      expectedKeys.exists(expected => found.equalsIgnoreCase(expected))
    )
    if (invalidKeys.nonEmpty) {
      throw new InvalidArgumentsException(
        s"Could not $operation with unrecognised option(s): ${invalidKeys.mkString("'", "', '", "'")}. Expected $VISIBLE_PERMITTED_OPTIONS."
      )
    }

    // Keys must be kept in sync with expectedKeys above!
    CreateDatabaseOptions(
      existingData = ExistingDataOption.findIn(optionsMap, config),
      databaseSeed = ExistingSeedInstanceOption.findIn(optionsMap, config),
      storeFormatNewDb = StoreFormatOption.findIn(optionsMap, config),
      seedURI = SeedURIOption.findIn(optionsMap, config),
      seedCredentials = SeedCredentialsOption.findIn(optionsMap, config),
      seedConfig = SeedConfigOption.findIn(optionsMap, config),
      txLogEnrichment = LogEnrichmentOption.findIn(optionsMap, config)
    )
  }

  implicit override def operation: String = "create database"
}

case object CreateCompositeDatabaseOptionsConverter extends OptionsConverter[CreateDatabaseOptions] {
  // Composite databases do not have any valid options, but allows for an empty options map

  override def convert(options: MapValue, config: Option[Config]): CreateDatabaseOptions = {
    if (!options.isEmpty)
      throw new InvalidArgumentsException(
        s"Could not $operation: composite databases have no valid options values."
      )
    CreateDatabaseOptions(None, None, None, None, None, None, None)
  }

  override def operation: String = s"create composite database"
}

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
    val maybeConfig = options.getOption("indexconfig")

    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider(_, schemaType, indexType))
    val configMap: java.util.Map[String, Object] =
      maybeConfig.map(assertValidAndTransformConfig(_, schemaType, indexProvider)).getOrElse(Collections.emptyMap())
    val indexConfig = IndexSettingUtil.toIndexConfigFromStringObjectMap(configMap)

    (indexProvider, indexConfig)
  }

  protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): java.util.Map[String, Object]

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

  protected def checkForPointConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (
      itemsMap.exists { case (p: String, _) =>
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_MAX.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MAX.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_MAX.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_3D_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_3D_MAX.getSettingName)
      }
    ) {
      itemsMap.writeTo(pp)
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index config '${pp.value()}', contains spatial config settings options.
           |To create point index, please use 'CREATE POINT INDEX ...'.""".stripMargin
      )
    }

  protected def checkForFulltextConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (
      itemsMap.exists { case (p, _) =>
        p.equalsIgnoreCase(FULLTEXT_ANALYZER.getSettingName) || p.equalsIgnoreCase(
          FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName
        )
      }
    ) {
      itemsMap.writeTo(pp)
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index config '${pp.value()}', contains fulltext config options.
           |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin
      )
    }

  protected def checkForVectorConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (
      itemsMap.exists { case (p, _) =>
        p.equalsIgnoreCase(VECTOR_DIMENSIONS.getSettingName) || p.equalsIgnoreCase(
          VECTOR_SIMILARITY_FUNCTION.getSettingName
        )
      }
    ) {
      itemsMap.writeTo(pp)
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index config '${pp.value()}', contains vector config options.
           |To create vector index, please use 'CREATE VECTOR INDEX ...'.""".stripMargin
      )
    }

  protected def assertEmptyConfig(
    config: AnyValue,
    schemaType: String,
    indexType: String
  ): java.util.Map[String, Object] = {
    // no available config settings, throw nice error when existing config settings for other index types
    val pp = new PrettyPrinter()
    config match {
      case itemsMap: MapValue =>
        checkForFulltextConfigValues(pp, itemsMap, schemaType)
        checkForPointConfigValues(pp, itemsMap, schemaType)
        checkForVectorConfigValues(pp, itemsMap, schemaType)

        if (!itemsMap.isEmpty) {
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create $schemaType with specified index config '${pp.value()}': $indexType indexes have no valid config values.""".stripMargin
          )
        }

        Collections.emptyMap()
      case unknown =>
        unknown.writeTo(pp)
        throw new InvalidArgumentsException(
          s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map."
        )
    }
  }
}

case class PropertyExistenceOrTypeConstraintOptionsConverter(
  entity: String,
  constraintType: String,
  context: QueryContext
) extends IndexOptionsConverter[CreateWithNoOptions] {
  // Property existence and property type constraints are not index-backed and do not have any valid options, but allows for an empty options map

  override def convert(options: MapValue, config: Option[Config]): CreateWithNoOptions = {
    if (!options.isEmpty)
      throw new InvalidArgumentsException(
        s"Could not create $entity property $constraintType constraint: property $constraintType constraints have no valid options values."
      )
    CreateWithNoOptions()
  }

  // No options available, this method doesn't get called
  override protected def assertValidAndTransformConfig(
    config: AnyValue,
    entity: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): java.util.Map[String, Object] = Collections.emptyMap()

  override def operation: String = s"create $entity property $constraintType constraint"
}

case class IndexBackedConstraintsOptionsConverter(schemaType: String, context: QueryContext)
    extends CreateRangeOptionsConverter(schemaType)

case class CreateRangeIndexOptionsConverter(schemaType: String, context: QueryContext)
    extends CreateRangeOptionsConverter(schemaType)

abstract class CreateRangeOptionsConverter(schemaType: String)
    extends IndexOptionsConverter[CreateIndexProviderOnlyOptions] {

  override def convert(options: MapValue, config: Option[Config]): CreateIndexProviderOnlyOptions = {
    val (indexProvider, _) = getOptionsParts(options, schemaType, IndexType.RANGE)
    CreateIndexProviderOnlyOptions(indexProvider)
  }

  // RANGE indexes has no available config settings
  override protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): java.util.Map[String, Object] =
    assertEmptyConfig(config, schemaType, "range")

  override def operation: String = s"create $schemaType"
}

case class CreateLookupIndexOptionsConverter(context: QueryContext)
    extends IndexOptionsConverter[CreateIndexProviderOnlyOptions] {
  private val schemaType = "token lookup index"

  override def convert(options: MapValue, config: Option[Config]): CreateIndexProviderOnlyOptions = {
    val (indexProvider, _) = getOptionsParts(options, schemaType, IndexType.LOOKUP)
    CreateIndexProviderOnlyOptions(indexProvider)
  }

  // LOOKUP indexes has no available config settings
  override protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): java.util.Map[String, Object] =
    assertEmptyConfig(config, schemaType, "lookup")

  override def operation: String = s"create $schemaType"
}

case class CreateFulltextIndexOptionsConverter(context: QueryContext)
    extends IndexOptionsConverter[CreateIndexWithFullOptions] {
  private val schemaType = "fulltext index"

  override def convert(options: MapValue, config: Option[Config]): CreateIndexWithFullOptions = {
    val (indexProvider, indexConfig) = getOptionsParts(options, schemaType, IndexType.FULLTEXT)
    CreateIndexWithFullOptions(indexProvider, indexConfig)
  }

  // FULLTEXT indexes have two config settings:
  //    current keys: fulltext.analyzer and fulltext.eventually_consistent
  //    current values: string and boolean
  override protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): java.util.Map[String, Object] = {

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(
        s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map from String to Strings and Booleans."
      )
    }

    config match {
      case itemsMap: MapValue =>
        checkForPointConfigValues(new PrettyPrinter(), itemsMap, schemaType)
        checkForVectorConfigValues(new PrettyPrinter(), itemsMap, schemaType)

        val hm = new java.util.HashMap[String, Object]()
        itemsMap.foreach {
          case (p: String, e: TextValue) =>
            hm.put(p, e.stringValue())
          case (p: String, e: BooleanValue) =>
            hm.put(p, java.lang.Boolean.valueOf(e.booleanValue()))
          case _ => throw exceptionWrongType(itemsMap)
        }
        hm
      case unknown =>
        throw exceptionWrongType(unknown)
    }
  }

  override def operation: String = s"create $schemaType"
}

case class CreateTextIndexOptionsConverter(context: QueryContext)
    extends IndexOptionsConverter[CreateIndexProviderOnlyOptions] {
  private val schemaType = "text index"

  override def convert(options: MapValue, config: Option[Config]): CreateIndexProviderOnlyOptions = {
    val (indexProvider, _) = getOptionsParts(options, schemaType, IndexType.TEXT)
    CreateIndexProviderOnlyOptions(indexProvider)
  }

  // TEXT indexes has no available config settings
  override protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): java.util.Map[String, Object] =
    assertEmptyConfig(config, schemaType, "text")

  override def operation: String = s"create $schemaType"
}

case class CreatePointIndexOptionsConverter(context: QueryContext)
    extends IndexOptionsConverter[CreateIndexWithFullOptions] {
  private val schemaType = "point index"

  override def convert(options: MapValue, config: Option[Config]): CreateIndexWithFullOptions = {
    val (indexProvider, indexConfig) = getOptionsParts(options, schemaType, IndexType.POINT)
    CreateIndexWithFullOptions(indexProvider, indexConfig)
  }

  // POINT indexes has point config settings
  override protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): java.util.Map[String, Object] = {
    // current keys: spatial.* (cartesian.|cartesian-3d.|wgs-84.|wgs-84-3d.) + (min|max)
    // current values: Double[]

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(
        s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map from String to Double[]."
      )
    }

    config match {
      case itemsMap: MapValue =>
        checkForFulltextConfigValues(new PrettyPrinter(), itemsMap, schemaType)
        checkForVectorConfigValues(new PrettyPrinter(), itemsMap, schemaType)

        itemsMap.foldLeft(Map[String, Object]()) {
          case (m, (p: String, e: ListValue)) =>
            val configValue: Array[Double] = e.iterator().asScala.map {
              case d: DoubleValue => d.doubleValue()
              case _              => throw exceptionWrongType(itemsMap)
            }.toArray
            m + (p -> configValue)
          case _ => throw exceptionWrongType(itemsMap)
        }.asJava
      case unknown =>
        throw exceptionWrongType(unknown)
    }
  }

  override def operation: String = s"create $schemaType"
}

case class CreateVectorIndexOptionsConverter(context: QueryContext)
    extends IndexOptionsConverter[CreateIndexWithFullOptions] {
  private val schemaType = "vector index"
  private val dimensionsSetting = VECTOR_DIMENSIONS.getSettingName
  private val similarityFunctionSetting = VECTOR_SIMILARITY_FUNCTION.getSettingName

  override protected val hasMandatoryOptions: Boolean = true

  override def convert(options: MapValue, config: Option[Config]): CreateIndexWithFullOptions = {
    val (indexProvider, indexConfig) = getOptionsParts(options, schemaType, IndexType.VECTOR)

    // We check that all mandatory settings are included if we get a config to throw errors on missing ones early.
    // However, the validate config method isn't called when no config is given
    // but this case should also give an error so we need to check that here.
    assertMandatoryConfigSettingsExists(indexConfig)

    CreateIndexWithFullOptions(indexProvider, indexConfig)
  }

  // VECTOR indexes has vector config settings
  override protected def assertValidAndTransformConfig(
    config: AnyValue,
    schemaType: String,
    indexProvider: Option[IndexProviderDescriptor]
  ): java.util.Map[String, Object] = {
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

        hm
      case unknown =>
        throw exceptionWrongType(unknown)
    }
  }

  private def assertMandatoryConfigSettingsExists(indexConfig: IndexConfig): Unit = {
    val settings = indexConfig.asMap().keySet().asScala.toSet
    // Call assert method to get the same error as for giving an empty setting map, without needing to duplicate it.
    // If there is a config it has already been checked as part of the validation.
    if (settings.isEmpty) {
      assertMandatoryConfigSettingsExists(settings)
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

case class CreateWithNoOptions()

case class CreateIndexProviderOnlyOptions(provider: Option[IndexProviderDescriptor])

case class CreateIndexWithFullOptions(provider: Option[IndexProviderDescriptor], config: IndexConfig)

case class CreateDatabaseOptions(
  existingData: Option[String],
  databaseSeed: Option[String],
  storeFormatNewDb: Option[String],
  seedURI: Option[String],
  seedCredentials: Option[String],
  seedConfig: Option[String],
  txLogEnrichment: Option[String]
)

case class AlterDatabaseOptions(txLogEnrichment: Option[String])

case class ServerOptions(
  allowed: Option[Set[NormalizedDatabaseName]],
  denied: Option[Set[NormalizedDatabaseName]],
  mode: Option[InstanceModeConstraint],
  tags: Option[List[String]]
)
