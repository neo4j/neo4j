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

import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.evaluator.Evaluator
import org.neo4j.cypher.internal.evaluator.ExpressionEvaluator
import org.neo4j.cypher.internal.expressions.Expression
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
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.kernel.impl.index.schema.PointIndexProvider
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider
import org.neo4j.kernel.impl.index.schema.TextIndexProviderFactory
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues

import java.util.Collections
import scala.collection.JavaConverters.asScalaIteratorConverter

trait OptionsConverter[T] {

  val evaluator: ExpressionEvaluator = Evaluator.expressionEvaluator()

  def evaluate(expression: Expression, params: MapValue): AnyValue = {
      evaluator.evaluate(expression, params)
  }

  def convert(options: Options, params: MapValue): Option[T] = options match {
    case NoOptions => None
    case OptionsMap(map) => Some(convert(VirtualValues.map(
      map.keys.map(_.toLowerCase).toArray,
      map.mapValues(evaluate(_, params)).values.toArray)))
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

case object CreateDatabaseOptionsConverter extends OptionsConverter[CreateDatabaseOptions] {
  val EXISTING_DATA = "existingData"
  val EXISTING_SEED_INSTANCE = "existingDataSeedInstance"
  val NUM_PRIMARIES = "primaries"
  val NUM_SECONDARIES = "secondaries"
  val VISIBLE_PERMITTED_OPTIONS = s"'$EXISTING_DATA', '$EXISTING_SEED_INSTANCE'"

  //existing Data values
  val USE_EXISTING_DATA = "use"

  override def convert(map: MapValue): CreateDatabaseOptions = {

      map.foldLeft(CreateDatabaseOptions(None, None, None, None)) { case (ops, (key, value)) =>
      //existingData
      if (key.equalsIgnoreCase(EXISTING_DATA)) {
        value match {
          case existingDataVal: TextValue if USE_EXISTING_DATA.equalsIgnoreCase(existingDataVal.stringValue()) => ops.copy(existingData = Some(USE_EXISTING_DATA))
          case value: TextValue =>
            throw new InvalidArgumentsException(s"Could not create database with specified $EXISTING_DATA '${value.stringValue()}'. Expected '$USE_EXISTING_DATA'.")
          case value: AnyValue =>
            throw new InvalidArgumentsException(s"Could not create database with specified $EXISTING_DATA '$value'. Expected '$USE_EXISTING_DATA'.")
        }

        //existingDataSeedInstance
      } else if (key.equalsIgnoreCase(EXISTING_SEED_INSTANCE)) {
        value match {
          case seed: TextValue => ops.copy(databaseSeed = Some(seed.stringValue()))
          case _ =>
            throw new InvalidArgumentsException(s"Could not create database with specified $EXISTING_SEED_INSTANCE '$value'. Expected instance uuid string.")
        }
        //numberOfPrimaries
      } else if (key.equalsIgnoreCase(NUM_PRIMARIES)) {
        value match {
          case number: IntegralValue if number.longValue() >= 1 => ops.copy(primaries = Some(number.longValue().intValue()))
          case _ =>
            throw new InvalidArgumentsException(s"Could not create database with specified $NUM_PRIMARIES '$value'. Expected positive integer number of primaries.")
        }
        //numberOfSecondaries
      } else if (key.equalsIgnoreCase(NUM_SECONDARIES)) {
        value match {
          case number: IntegralValue if number.longValue() >= 0 => ops.copy(secondaries = Some(number.longValue().intValue()))
          case _ =>
            throw new InvalidArgumentsException(s"Could not create database with specified $NUM_SECONDARIES '$value'. Expected non-negative integer number of secondaries.")
        }
      } else {
        throw new InvalidArgumentsException(s"Could not create database with unrecognised option: '$key'. Expected $VISIBLE_PERMITTED_OPTIONS.")
      }
    }
  }

  override def operation: String = "create database"
}

trait IndexOptionsConverter[T] extends OptionsConverter[T] {
  def getOptionsParts(options: MapValue, schemaType: String): (Option[AnyValue], IndexConfig) = {

    if (options.exists{ case (k,_) => !k.equalsIgnoreCase("indexProvider") && !k.equalsIgnoreCase("indexConfig")}) {
      throw new InvalidArgumentsException(s"Failed to create $schemaType: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
    }
    val maybeIndexProvider = options.getOption("indexprovider")
    val maybeConfig = options.getOption("indexconfig")

    val configMap: java.util.Map[String, Object] = maybeConfig.map(assertValidAndTransformConfig(_, schemaType)).getOrElse(Collections.emptyMap())
    val indexConfig = IndexSettingUtil.toIndexConfigFromStringObjectMap(configMap)

    (maybeIndexProvider, indexConfig)
  }

  def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object]

  protected def checkForBtreeProvider(indexProviderString: String, schemaType: String): Unit =
    if (indexProviderString.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) ||
      indexProviderString.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()))
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index provider '$indexProviderString'.
           |To create btree index, please use 'CREATE BTREE INDEX ...'.""".stripMargin)

  protected def checkForRangeProvider(indexProviderString: String, schemaType: String): Unit =
    if (indexProviderString.equalsIgnoreCase(RangeIndexProvider.DESCRIPTOR.name()))
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index provider '$indexProviderString'.
           |To create range index, please use 'CREATE RANGE INDEX ...'.""".stripMargin)

  protected def checkForFulltextProvider(indexProviderString: String, schemaType: String): Unit =
    if (indexProviderString.equalsIgnoreCase(FulltextIndexProviderFactory.DESCRIPTOR.name()))
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index provider '$indexProviderString'.
           |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin)

  protected def checkForTokenLookupProvider(indexProviderString: String, schemaType: String): Unit =
    if (indexProviderString.equalsIgnoreCase(TokenIndexProvider.DESCRIPTOR.name()))
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index provider '$indexProviderString'.
           |To create token lookup index, please use 'CREATE LOOKUP INDEX ...'.""".stripMargin)

  protected def checkForTextProvider(indexProviderString: String, schemaType: String): Unit =
    if (indexProviderString.equalsIgnoreCase(TextIndexProviderFactory.DESCRIPTOR.name()))
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index provider '$indexProviderString'.
           |To create text index, please use 'CREATE TEXT INDEX ...'.""".stripMargin)

  protected def checkForPointProvider(indexProviderString: String, schemaType: String): Unit =
    if (indexProviderString.equalsIgnoreCase(PointIndexProvider.DESCRIPTOR.name()))
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index provider '$indexProviderString'.
           |To create point index, please use 'CREATE POINT INDEX ...'.""".stripMargin)

  protected def checkForPointConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (itemsMap.exists { case (p: String, _) =>
      p.equalsIgnoreCase(SPATIAL_CARTESIAN_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_MAX.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MAX.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_MAX.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_3D_MIN.getSettingName) ||
        p.equalsIgnoreCase(SPATIAL_WGS84_3D_MAX.getSettingName)
    }) {
      itemsMap.writeTo(pp)
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index config '${pp.value()}', contains spatial config settings options.
           |To create btree index, please use 'CREATE BTREE INDEX ...' and to create point index, please use 'CREATE POINT INDEX ...'.""".stripMargin)
    }

  protected def checkForFulltextConfigValues(pp: PrettyPrinter, itemsMap: MapValue, schemaType: String): Unit =
    if (itemsMap.exists { case (p, _) => p.equalsIgnoreCase(FULLTEXT_ANALYZER.getSettingName) || p.equalsIgnoreCase(FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName) }) {
      itemsMap.writeTo(pp)
      throw new InvalidArgumentsException(
        s"""Could not create $schemaType with specified index config '${pp.value()}', contains fulltext config options.
           |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin)
    }

  protected def assertEmptyConfig(config: AnyValue, schemaType: String, indexType: String): java.util.Map[String, Object] = {
    // no available config settings, throw nice error when existing config settings for other index types
    val pp = new PrettyPrinter()
    config match {
      case itemsMap: MapValue =>
        checkForFulltextConfigValues(pp, itemsMap, schemaType)
        checkForPointConfigValues(pp, itemsMap, schemaType)

        if(!itemsMap.isEmpty){
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create $schemaType with specified index config '${pp.value()}': $indexType indexes have no valid config values.""".stripMargin)
        }

        Collections.emptyMap()
      case unknown =>
        unknown.writeTo(pp)
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map.")
    }
  }

  protected def assertValidAndTransformConfigForPointSettings(config: AnyValue, schemaType: String): java.util.Map[String, Object] = {
    // current keys: spatial.* (cartesian.|cartesian-3d.|wgs-84.|wgs-84-3d.) + (min|max)
    // current values: Double[]

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map from String to Double[].")
    }

    config match {
      case itemsMap: MapValue =>
        checkForFulltextConfigValues(new PrettyPrinter(), itemsMap, schemaType)

        val hm = new java.util.HashMap[String, Array[Double]]()
        itemsMap.foreach {
          case (p: String, e: ListValue) =>
            val configValue: Array[Double] = e.iterator().asScala.map {
              case d: DoubleValue => d.doubleValue()
              case _ => throw exceptionWrongType(itemsMap)
            }.toArray
            hm.put(p, configValue)
          case _ => throw exceptionWrongType(itemsMap)
        }
        hm.asInstanceOf[java.util.Map[String, Object]]
      case unknown =>
        throw exceptionWrongType(unknown)
    }
  }
}

case class PropertyExistenceConstraintOptionsConverter(entity: String) extends IndexOptionsConverter[CreateWithNoOptions] {
  // Property existence constraints are not index-backed and do not have any valid options, but allows for an empty options map

  override def convert(options: MapValue): CreateWithNoOptions = {
    if (!options.isEmpty)
      throw new InvalidArgumentsException(s"Could not create $entity property existence constraint: property existence constraints have no valid options values.")
    CreateWithNoOptions()
  }

  // No options available, this method doesn't get called
  override def assertValidAndTransformConfig(config: AnyValue, entity: String): java.util.Map[String, Object] = Collections.emptyMap()

  override def operation: String = s"create $entity property existence constraint"
}

case class IndexBackedConstraintsOptionsConverter(schemaType: String) extends IndexOptionsConverter[CreateIndexWithStringProviderOptions] {

  override def convert(options: MapValue): CreateIndexWithStringProviderOptions = {
    val (maybeIndexProvider, indexConfig) = getOptionsParts(options, schemaType)
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    assertOnlyOneIndexTypeOptions(indexProvider, indexConfig)
    CreateIndexWithStringProviderOptions(indexProvider, indexConfig)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): String = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()
      checkForFulltextProvider(indexProviderString, schemaType)
      checkForTokenLookupProvider(indexProviderString, schemaType)
      checkForTextProvider(indexProviderString, schemaType)
      checkForPointProvider(indexProviderString, schemaType)

      if (!indexProviderString.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) &&
        !indexProviderString.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()) &&
        !indexProviderString.equalsIgnoreCase(RangeIndexProvider.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderString'.")

      indexProviderString

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value.")
  }

  private def assertOnlyOneIndexTypeOptions(indexProvider: Option[String], indexConfig: IndexConfig): Unit = indexProvider match {
    // indexProvider RANGE has no available config settings (called after assertValidAndTransformConfig)
    case Some(provider) if provider.equalsIgnoreCase(RangeIndexProvider.DESCRIPTOR.name()) && !indexConfig.asMap().isEmpty =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified options: range indexes have no available configuration settings.")
    case _ =>
  }

  // RANGE indexes has no available config settings, check conforms to BTREE config (point settings)
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] =
    assertValidAndTransformConfigForPointSettings(config, schemaType)

  override def operation: String = s"create $schemaType"
}

case class CreateBtreeIndexOptionsConverter(schemaType: String) extends IndexOptionsConverter[CreateIndexWithStringProviderOptions] {

  override def convert(options: MapValue): CreateIndexWithStringProviderOptions = {
    val (maybeIndexProvider, indexConfig) = getOptionsParts(options, schemaType)
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateIndexWithStringProviderOptions(indexProvider, indexConfig)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): String = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()
      checkForFulltextProvider(indexProviderString, schemaType)
      checkForRangeProvider(indexProviderString, schemaType)
      checkForTokenLookupProvider(indexProviderString, schemaType)
      checkForTextProvider(indexProviderString, schemaType)
      checkForPointProvider(indexProviderString, schemaType)

      if (!indexProviderString.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) &&
        !indexProviderString.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderString'.")

      indexProviderString

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value.")
  }

  // BTREE indexes has point config settings
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] =
    assertValidAndTransformConfigForPointSettings(config, schemaType)

  override def operation: String = s"create $schemaType"
}

case class CreateRangeIndexOptionsConverter(schemaType: String) extends IndexOptionsConverter[CreateIndexProviderOnlyOptions] {

  override def convert(options: MapValue): CreateIndexProviderOnlyOptions = {
    val (maybeIndexProvider, _) = getOptionsParts(options, schemaType)
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateIndexProviderOnlyOptions(indexProvider)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): IndexProviderDescriptor  = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()
      checkForFulltextProvider(indexProviderString, schemaType)
      checkForBtreeProvider(indexProviderString, schemaType)
      checkForTokenLookupProvider(indexProviderString, schemaType)
      checkForTextProvider(indexProviderString, schemaType)
      checkForPointProvider(indexProviderString, schemaType)

      if (!indexProviderString.equalsIgnoreCase(RangeIndexProvider.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderString'.")

      RangeIndexProvider.DESCRIPTOR

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value.")
  }

  // RANGE indexes has no available config settings
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] =
    assertEmptyConfig(config, schemaType, "range")

  override def operation: String = s"create $schemaType"
}

case object CreateLookupIndexOptionsConverter extends IndexOptionsConverter[CreateIndexProviderOnlyOptions] {
  private val schemaType = "token lookup index"

  override def convert(options: MapValue): CreateIndexProviderOnlyOptions =  {
    val (maybeIndexProvider, _) = getOptionsParts(options, schemaType)
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateIndexProviderOnlyOptions(indexProvider)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()
      checkForFulltextProvider(indexProviderString, schemaType)
      checkForBtreeProvider(indexProviderString, schemaType)
      checkForRangeProvider(indexProviderString, schemaType)
      checkForTextProvider(indexProviderString, schemaType)
      checkForPointProvider(indexProviderString, schemaType)

      if (!indexProviderString.equalsIgnoreCase(TokenIndexProvider.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderString'.")

      TokenIndexProvider.DESCRIPTOR

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value.")
  }

  // LOOKUP indexes has no available config settings
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] =
    assertEmptyConfig(config, schemaType, "lookup")

  override def operation: String = s"create $schemaType"
}

case object CreateFulltextIndexOptionsConverter extends IndexOptionsConverter[CreateIndexWithProviderDescriptorOptions] {
  private val schemaType = "fulltext index"

  override def convert(options: MapValue): CreateIndexWithProviderDescriptorOptions =  {
    val (maybeIndexProvider, indexConfig) = getOptionsParts(options, schemaType)
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateIndexWithProviderDescriptorOptions(indexProvider, indexConfig)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()
      checkForBtreeProvider(indexProviderString, schemaType)
      checkForRangeProvider(indexProviderString, schemaType)
      checkForTokenLookupProvider(indexProviderString, schemaType)
      checkForTextProvider(indexProviderString, schemaType)
      checkForPointProvider(indexProviderString, schemaType)

      if (!indexProviderString.equalsIgnoreCase(FulltextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderString'.")

      FulltextIndexProviderFactory.DESCRIPTOR

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value.")
  }

  // FULLTEXT indexes have two config settings:
  //    current keys: fulltext.analyzer and fulltext.eventually_consistent
  //    current values: string and boolean
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] = {

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map from String to Strings and Booleans.")
    }

    config match {
      case itemsMap: MapValue =>
        checkForPointConfigValues(new PrettyPrinter(), itemsMap, schemaType)

        val hm = new java.util.HashMap[String, Object]()
        itemsMap.foreach {
          case (p: String, e: TextValue)  =>
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

case object CreateTextIndexOptionsConverter extends IndexOptionsConverter[CreateIndexProviderOnlyOptions] {
  private val schemaType = "text index"

  override def convert(options: MapValue): CreateIndexProviderOnlyOptions =  {
    val (maybeIndexProvider, _) = getOptionsParts(options, schemaType)
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateIndexProviderOnlyOptions(indexProvider)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()
      checkForFulltextProvider(indexProviderString, schemaType)
      checkForBtreeProvider(indexProviderString, schemaType)
      checkForRangeProvider(indexProviderString, schemaType)
      checkForTokenLookupProvider(indexProviderString, schemaType)
      checkForPointProvider(indexProviderString, schemaType)

      if (!indexProviderString.equalsIgnoreCase(TextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderString'.")

      TextIndexProviderFactory.DESCRIPTOR

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value.")
  }

  // TEXT indexes has no available config settings
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] =
    assertEmptyConfig(config, schemaType, "text")

  override def operation: String = s"create $schemaType"
}

case object CreatePointIndexOptionsConverter extends IndexOptionsConverter[CreateIndexWithProviderDescriptorOptions] {
  private val schemaType = "point index"

  override def convert(options: MapValue): CreateIndexWithProviderDescriptorOptions = {
    val (maybeIndexProvider, indexConfig) = getOptionsParts(options, schemaType)
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateIndexWithProviderDescriptorOptions(indexProvider, indexConfig)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()
      checkForFulltextProvider(indexProviderString, schemaType)
      checkForBtreeProvider(indexProviderString, schemaType)
      checkForRangeProvider(indexProviderString, schemaType)
      checkForTokenLookupProvider(indexProviderString, schemaType)
      checkForTextProvider(indexProviderString, schemaType)

      if (!indexProviderString.equalsIgnoreCase(PointIndexProvider.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderString'.")

      PointIndexProvider.DESCRIPTOR

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value.")
  }

  // POINT indexes has point config settings
  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] =
    assertValidAndTransformConfigForPointSettings(config, schemaType)

  override def operation: String = s"create $schemaType"
}

case class CreateWithNoOptions()
case class CreateIndexProviderOnlyOptions(provider: Option[IndexProviderDescriptor])
case class CreateIndexWithStringProviderOptions(provider: Option[String], config: IndexConfig)
case class CreateIndexWithProviderDescriptorOptions(provider: Option[IndexProviderDescriptor], config: IndexConfig)
case class CreateDatabaseOptions(existingData: Option[String], databaseSeed: Option[String], primaries: Option[Integer], secondaries: Option[Integer])

object MapValueOps {

  implicit class Ops(mv: MapValue) extends Map[String, AnyValue] {

    def getOption(key: String): Option[AnyValue] = mv.get(key) match {
      case _: NoValue => None
      case value => Some(value)
    }

    override def +[V1 >: AnyValue](kv: (String, V1)): Map[String, V1] = {
      val mvb = new MapValueBuilder()
      mv.foreach((k,v) => mvb.add(k, v))
      mvb.add(kv._1, kv._2.asInstanceOf[AnyValue])
      mvb.build()
    }

    override def get(key: String): Option[AnyValue] = getOption(key)

    override def iterator: Iterator[(String, AnyValue)] = {
      val keys = mv.keySet().iterator()
      new Iterator[(String, AnyValue)] {
        override def hasNext: Boolean = keys.hasNext

        override def next(): (String, AnyValue) = {
          val k = keys.next()
          (k, mv.get(k))
        }
      }
    }

    override def -(key: String): Map[String, AnyValue] = {
      val mvb = new MapValueBuilder()
      mv.foreach((k,v) => if (!k.equals(key)) mvb.add(k,v))
      mvb.build()
    }
  }
}
