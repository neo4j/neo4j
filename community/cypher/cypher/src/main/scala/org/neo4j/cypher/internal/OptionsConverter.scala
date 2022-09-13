/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.configuration.Config
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
import org.neo4j.graphdb.schema.IndexSettingUtil
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues

import java.util.Collections

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

trait OptionsConverter[T] {

  val evaluator: ExpressionEvaluator = Evaluator.expressionEvaluator()

  def evaluate(expression: Expression, params: MapValue): AnyValue = {
    evaluator.evaluate(expression, params)
  }

  def convert(options: Options, params: MapValue): Option[T] = options match {
    case NoOptions => None
    case OptionsMap(map) => Some(convert(VirtualValues.map(
        map.keys.map(_.toLowerCase).toArray,
        map.view.mapValues(evaluate(_, params)).values.toArray
      )))
    case OptionsParam(parameter) =>
      val opsMap = params.get(parameter.name)
      opsMap match {
        case mv: MapValue =>
          val builder = new MapValueBuilder()
          mv.foreach((k, v) => builder.add(k.toLowerCase(), v))
          Some(convert(builder.build()))
        case _ =>
          throw new InvalidArgumentsException(s"Could not $operation with options '$opsMap'. Expected a map value.")
      }
  }

  def operation: String

  def convert(options: MapValue): T
}

case object ServerOptionsConverter extends OptionsConverter[ServerOptions] {
  private val ALLOWED_DATABASES = "allowedDatabases"
  private val DENIED_DATABASES = "deniedDatabases"
  private val MODE_CONSTRAINT = "modeConstraint"

  val VISIBLE_PERMITTED_OPTIONS = s"'$ALLOWED_DATABASES', '$DENIED_DATABASES', '$MODE_CONSTRAINT'"

  override def operation: String = "enable server"

  override def convert(map: MapValue): ServerOptions = {
    map.foldLeft(ServerOptions(None, None, None)) {
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
                  InstanceModeConstraint.valueOf(t.stringValue().toUpperCase)
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
        } else {
          throw new InvalidArgumentsException(
            s"Unrecognised option '$key', expected $VISIBLE_PERMITTED_OPTIONS."
          )
        }
    }
  }
}

case object CreateDatabaseOptionsConverter extends OptionsConverter[CreateDatabaseOptions] {
  val EXISTING_DATA = "existingData"
  val EXISTING_SEED_INSTANCE = "existingDataSeedInstance"
  val NUM_PRIMARIES = "primaries"
  val NUM_SECONDARIES = "secondaries"
  val STORE_FORMAT = "storeFormat"
  val SEED_URI = "seedURI"
  val SEED_CREDENTIALS = "seedCredentials"
  val SEED_CONFIG = "seedConfig"
  val VISIBLE_PERMITTED_OPTIONS = s"'$EXISTING_DATA', '$EXISTING_SEED_INSTANCE', '$STORE_FORMAT'"

  // existing Data values
  val USE_EXISTING_DATA = "use"

  override def convert(map: MapValue): CreateDatabaseOptions = {

    map.foldLeft(CreateDatabaseOptions(None, None, None, None, None, None, None, None)) {
      case (ops, (key, value)) =>
        // existingData
        if (key.equalsIgnoreCase(EXISTING_DATA)) {
          value match {
            case existingDataVal: TextValue if USE_EXISTING_DATA.equalsIgnoreCase(existingDataVal.stringValue()) =>
              ops.copy(existingData = Some(USE_EXISTING_DATA))
            case value: TextValue =>
              throw new InvalidArgumentsException(
                s"Could not create database with specified $EXISTING_DATA '${value.stringValue()}'. Expected '$USE_EXISTING_DATA'."
              )
            case value: AnyValue =>
              throw new InvalidArgumentsException(
                s"Could not create database with specified $EXISTING_DATA '$value'. Expected '$USE_EXISTING_DATA'."
              )
          }

          // existingDataSeedInstance
        } else if (key.equalsIgnoreCase(EXISTING_SEED_INSTANCE)) {
          value match {
            case seed: TextValue => ops.copy(databaseSeed = Some(seed.stringValue()))
            case _ =>
              throw new InvalidArgumentsException(
                s"Could not create database with specified $EXISTING_SEED_INSTANCE '$value'. Expected server uuid string."
              )
          }
          // primariesCount
        } else if (key.equalsIgnoreCase(NUM_PRIMARIES)) {
          value match {
            case number: IntegralValue if number.longValue() >= 1 =>
              ops.copy(primaries = Some(number.longValue().intValue()))
            case _ =>
              throw new InvalidArgumentsException(
                s"Could not create database with specified $NUM_PRIMARIES '$value'. Expected positive integer number of primaries."
              )
          }
          // secondariesCount
        } else if (key.equalsIgnoreCase(NUM_SECONDARIES)) {
          value match {
            case number: IntegralValue if number.longValue() >= 0 =>
              ops.copy(secondaries = Some(number.longValue().intValue()))
            case _ =>
              throw new InvalidArgumentsException(
                s"Could not create database with specified $NUM_SECONDARIES '$value'. Expected non-negative integer number of secondaries."
              )
          }
          // storeFormat
        } else if (key.equalsIgnoreCase(STORE_FORMAT)) {
          value match {
            case storeFormat: TextValue =>
              try {
                // Validate the format by looking for a storage engine that supports it - will throw if none was found
                StorageEngineFactory.selectStorageEngine(Config.defaults(
                  GraphDatabaseSettings.db_format,
                  storeFormat.stringValue()
                ))
                ops.copy(storeFormatNewDb = Some(storeFormat.stringValue()))
              } catch {
                case _: Exception =>
                  throw new InvalidArgumentsException(
                    s"Could not create database with specified $STORE_FORMAT '${storeFormat.stringValue()}'. Unknown format, supported formats are "
                      + "'aligned', 'standard' or 'high_limit'"
                  )
              }
            case _ =>
              throw new InvalidArgumentsException(
                s"Could not create database with specified $STORE_FORMAT '$value', String expected."
              )
          }
        }
        // seed URI
        else if (key.equalsIgnoreCase(SEED_URI)) {
          value match {
            case seedURI: TextValue => ops.copy(seedURI = Some(seedURI.stringValue()))
            case _ =>
              throw new InvalidArgumentsException(
                s"Could not create database with specified $SEED_URI '$value', String expected."
              )
          }
        } else if (key.equalsIgnoreCase(SEED_CREDENTIALS)) {
          value match {
            case seedCredentials: TextValue => ops.copy(seedCredentials = Some(seedCredentials.stringValue()))
            case _ =>
              throw new InvalidArgumentsException(
                s"Could not create database with specified $SEED_CREDENTIALS '$value', String expected."
              )
          }
        } else if (key.equalsIgnoreCase(SEED_CONFIG)) {
          value match {
            case seedConfig: TextValue => ops.copy(seedConfig = Some(seedConfig.stringValue()))
            case _ =>
              throw new InvalidArgumentsException(
                s"Could not create database with specified $SEED_CONFIG '$value', String expected."
              )
          }
        } else {
          throw new InvalidArgumentsException(
            s"Could not create database with unrecognised option: '$key'. Expected $VISIBLE_PERMITTED_OPTIONS."
          )
        }
    }
  }

  override def operation: String = "create database"
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

    val indexProvider = maybeIndexProvider.map(p => assertValidIndexProvider(p, schemaType, indexType))
    val configMap: java.util.Map[String, Object] =
      maybeConfig.map(assertValidAndTransformConfig(_, schemaType)).getOrElse(Collections.emptyMap())
    val indexConfig = IndexSettingUtil.toIndexConfigFromStringObjectMap(configMap)

    (indexProvider, indexConfig)
  }

  protected def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object]

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

case class PropertyExistenceConstraintOptionsConverter(entity: String, context: QueryContext)
    extends IndexOptionsConverter[CreateWithNoOptions] {
  // Property existence constraints are not index-backed and do not have any valid options, but allows for an empty options map

  override def convert(options: MapValue): CreateWithNoOptions = {
    if (!options.isEmpty)
      throw new InvalidArgumentsException(
        s"Could not create $entity property existence constraint: property existence constraints have no valid options values."
      )
    CreateWithNoOptions()
  }

  // No options available, this method doesn't get called
  override def assertValidAndTransformConfig(config: AnyValue, entity: String): java.util.Map[String, Object] =
    Collections.emptyMap()

  override def operation: String = s"create $entity property existence constraint"
}

case class IndexBackedConstraintsOptionsConverter(schemaType: String, context: QueryContext)
    extends CreateRangeOptionsConverter(schemaType)

case class CreateRangeIndexOptionsConverter(schemaType: String, context: QueryContext)
    extends CreateRangeOptionsConverter(schemaType)

abstract class CreateRangeOptionsConverter(schemaType: String)
    extends IndexOptionsConverter[CreateIndexProviderOnlyOptions] {

  override def convert(options: MapValue): CreateIndexProviderOnlyOptions = {
    val (indexProvider, _) = getOptionsParts(options, schemaType, IndexType.RANGE)
    CreateIndexProviderOnlyOptions(indexProvider)
  }

  // RANGE indexes has no available config settings
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] =
    assertEmptyConfig(config, schemaType, "range")

  override def operation: String = s"create $schemaType"
}

case class CreateLookupIndexOptionsConverter(context: QueryContext)
    extends IndexOptionsConverter[CreateIndexProviderOnlyOptions] {
  private val schemaType = "token lookup index"

  override def convert(options: MapValue): CreateIndexProviderOnlyOptions = {
    val (indexProvider, _) = getOptionsParts(options, schemaType, IndexType.LOOKUP)
    CreateIndexProviderOnlyOptions(indexProvider)
  }

  // LOOKUP indexes has no available config settings
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] =
    assertEmptyConfig(config, schemaType, "lookup")

  override def operation: String = s"create $schemaType"
}

case class CreateFulltextIndexOptionsConverter(context: QueryContext)
    extends IndexOptionsConverter[CreateIndexWithFullOptions] {
  private val schemaType = "fulltext index"

  override def convert(options: MapValue): CreateIndexWithFullOptions = {
    val (indexProvider, indexConfig) = getOptionsParts(options, schemaType, IndexType.FULLTEXT)
    CreateIndexWithFullOptions(indexProvider, indexConfig)
  }

  // FULLTEXT indexes have two config settings:
  //    current keys: fulltext.analyzer and fulltext.eventually_consistent
  //    current values: string and boolean
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] = {

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

  override def convert(options: MapValue): CreateIndexProviderOnlyOptions = {
    val (indexProvider, _) = getOptionsParts(options, schemaType, IndexType.TEXT)
    CreateIndexProviderOnlyOptions(indexProvider)
  }

  // TEXT indexes has no available config settings
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] =
    assertEmptyConfig(config, schemaType, "text")

  override def operation: String = s"create $schemaType"
}

case class CreatePointIndexOptionsConverter(context: QueryContext)
    extends IndexOptionsConverter[CreateIndexWithFullOptions] {
  private val schemaType = "point index"

  override def convert(options: MapValue): CreateIndexWithFullOptions = {
    val (indexProvider, indexConfig) = getOptionsParts(options, schemaType, IndexType.POINT)
    CreateIndexWithFullOptions(indexProvider, indexConfig)
  }

  // POINT indexes has point config settings
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] = {
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

case class CreateWithNoOptions()
case class CreateIndexProviderOnlyOptions(provider: Option[IndexProviderDescriptor])
case class CreateIndexWithFullOptions(provider: Option[IndexProviderDescriptor], config: IndexConfig)

case class CreateDatabaseOptions(
  existingData: Option[String],
  databaseSeed: Option[String],
  primaries: Option[Integer],
  secondaries: Option[Integer],
  storeFormatNewDb: Option[String],
  seedURI: Option[String],
  seedCredentials: Option[String],
  seedConfig: Option[String]
)

case class ServerOptions(
  allowed: Option[Set[NormalizedDatabaseName]],
  denied: Option[Set[NormalizedDatabaseName]],
  mode: Option[InstanceModeConstraint]
)
