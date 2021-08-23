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
import org.neo4j.kernel.impl.index.schema.TextIndexProviderFactory
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DoubleValue
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
  val PERMITTED_OPTIONS = s"'$EXISTING_DATA', '$EXISTING_SEED_INSTANCE'"

  //existing Data values
  val USE_EXISTING_DATA = "use"

  override def convert(map: MapValue): CreateDatabaseOptions = {

      map.foldLeft(CreateDatabaseOptions(None, None)) { case (ops, (key, value)) =>
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
            throw new InvalidArgumentsException(s"Could not create database with specified $EXISTING_SEED_INSTANCE '$value'. Expected database uuid string.")
        }
      } else {
        throw new InvalidArgumentsException(s"Could not create database with unrecognised option: '$key'. Expected $PERMITTED_OPTIONS.")
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
}

case class PropertyExistenceConstraintOptionsConverter(entity: String) extends IndexOptionsConverter[CreateWithNoOptions] {
  // Property existence constraints are not index-backed and do not have any valid options, but allows for an empty options map

  override def convert(options: MapValue): CreateWithNoOptions = {
    if (!options.isEmpty)
      throw new InvalidArgumentsException(s"Could not create $entity property existence constraint: property existence constraints have no valid options values.")
    CreateWithNoOptions()
  }

  override def assertValidAndTransformConfig(config: AnyValue, entity: String): java.util.Map[String, Object] = Collections.emptyMap()

  override def operation: String = s"create $entity property existence constraint"
}

case class CreateBtreeIndexOptionsConverter(schemaType: String) extends IndexOptionsConverter[CreateBtreeIndexOptions] {

  override def convert(options: MapValue): CreateBtreeIndexOptions = {
    val (maybeIndexProvider, indexConfig) = getOptionsParts(options, schemaType)
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateBtreeIndexOptions(indexProvider, indexConfig)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): String = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()

      if (indexProviderString.equalsIgnoreCase(FulltextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create $schemaType with specified index provider '$indexProviderString'.
             |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin)

      if (indexProviderString.equalsIgnoreCase(TokenIndexProvider.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create $schemaType with specified index provider '$indexProviderString'.
             |To create token lookup index, please use 'CREATE LOOKUP INDEX ...'.""".stripMargin)

      if (indexProviderString.equalsIgnoreCase(TextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create $schemaType with specified index provider '$indexProviderString'.
             |To create text index, please use 'CREATE TEXT INDEX ...'.""".stripMargin)

      if (!indexProviderString.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) &&
        !indexProviderString.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderString'.")

      indexProviderString

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProvider'. Expected String value.")
  }

  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] = {

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map from String to Double[].")
    }

    // for indexProvider BTREE:
    //    current keys: spatial.* (cartesian.|cartesian-3d.|wgs-84.|wgs-84-3d.) + (min|max)
    //    current values: Double[]
    config match {
      case itemsMap: MapValue =>
        if (itemsMap.exists { case (p, _) => p.equalsIgnoreCase(FULLTEXT_ANALYZER.getSettingName) || p.equalsIgnoreCase(FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName) }) {
          val pp = new PrettyPrinter()
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create $schemaType with specified index config '${pp.value()}', contains fulltext config options.
               |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin)
        }

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

  override def operation: String = s"create $schemaType"
}

case object CreateLookupIndexOptionsConverter extends IndexOptionsConverter[CreateProviderOnlyIndexOptions] {

  override def convert(options: MapValue): CreateProviderOnlyIndexOptions =  {
    val (maybeIndexProvider, _) = getOptionsParts(options, "token lookup index")
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateProviderOnlyIndexOptions(indexProvider)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()

      if (indexProviderString.equalsIgnoreCase(FulltextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create token lookup index with specified index provider '$indexProviderString'.
             |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin)

      if (indexProviderString.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) ||
        indexProviderString.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create token lookup index with specified index provider '$indexProviderString'.
             |To create btree index, please use 'CREATE BTREE INDEX ...'.""".stripMargin)

      if (indexProviderString.equalsIgnoreCase(TextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create token lookup index with specified index provider '$indexProviderString'.
             |To create text index, please use 'CREATE TEXT INDEX ...'.""".stripMargin)

      if (!indexProviderString.equalsIgnoreCase(TokenIndexProvider.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create token lookup index with specified index provider '$indexProviderString'.")

      TokenIndexProvider.DESCRIPTOR

    case _ =>
      throw new InvalidArgumentsException(s"Could not create token lookup index with specified index provider '$indexProvider'. Expected String value.")
  }

  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] = {
    // for indexProvider LOOKUP: no available config settings
    val pp = new PrettyPrinter()
    config match {
      case itemsMap: MapValue =>
        if (itemsMap.exists { case (p, _) => p.equalsIgnoreCase(FULLTEXT_ANALYZER.getSettingName) || p.equalsIgnoreCase(FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName) }) {
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create token lookup index with specified index config '${pp.value()}', contains fulltext config options.
               |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin)
        }

        if (itemsMap.exists { case (p: String, _) =>
          p.equalsIgnoreCase(SPATIAL_CARTESIAN_MIN.getSettingName) ||
            p.equalsIgnoreCase(SPATIAL_CARTESIAN_MAX.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MIN.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MAX.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_MIN.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_MAX.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_3D_MIN.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_3D_MAX.getSettingName)
        }) {
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create token lookup index with specified index config '${pp.value()}', contains btree config options.
               |To create btree index, please use 'CREATE BTREE INDEX ...'.""".stripMargin)
        }

        if(!itemsMap.isEmpty){
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create token lookup index with specified index config '${pp.value()}': lookup indexes have no valid config values.""".stripMargin)
        }

        Collections.emptyMap()
      case unknown =>
        unknown.writeTo(pp)
        throw new InvalidArgumentsException(s"Could not create token lookup index with specified index config '${pp.value()}'. Expected a map.")
    }
  }

  override def operation: String = "create token lookup index"
}

case object CreateFulltextIndexOptionsConverter extends IndexOptionsConverter[CreateFulltextIndexOptions] {

  override def convert(options: MapValue): CreateFulltextIndexOptions =  {
    val (maybeIndexProvider, indexConfig) = getOptionsParts(options, "fulltext index")
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateFulltextIndexOptions(indexProvider, indexConfig)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()

      if (indexProviderString.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) ||
        indexProviderString.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create fulltext index with specified index provider '$indexProviderString'.
             |To create btree index, please use 'CREATE BTREE INDEX ...'.""".stripMargin)

      if (indexProviderString.equalsIgnoreCase(TokenIndexProvider.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create fulltext index with specified index provider '$indexProviderString'.
             |To create token lookup index, please use 'CREATE LOOKUP INDEX ...'.""".stripMargin)

      if (indexProviderString.equalsIgnoreCase(TextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create fulltext index with specified index provider '$indexProviderString'.
             |To create text index, please use 'CREATE TEXT INDEX ...'.""".stripMargin)

      if (!indexProviderString.equalsIgnoreCase(FulltextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create fulltext index with specified index provider '$indexProviderString'.")

      FulltextIndexProviderFactory.DESCRIPTOR

    case _ =>
      throw new InvalidArgumentsException(s"Could not create fulltext index with specified index provider '$indexProvider'. Expected String value.")
  }

  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] = {

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(s"Could not create fulltext index with specified index config '${pp.value()}'. Expected a map from String to Strings and Booleans.")
    }

    // for indexProvider FULLTEXT:
    //    current keys: fulltext.analyzer and fulltext.eventually_consistent
    //    current values: string and boolean
    config match {
      case itemsMap: MapValue =>
        if (itemsMap.exists { case (p: String, _) =>
          p.equalsIgnoreCase(SPATIAL_CARTESIAN_MIN.getSettingName) ||
            p.equalsIgnoreCase(SPATIAL_CARTESIAN_MAX.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MIN.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MAX.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_MIN.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_MAX.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_3D_MIN.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_3D_MAX.getSettingName)
        }) {
          val pp = new PrettyPrinter()
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create fulltext index with specified index config '${pp.value()}', contains btree config options.
               |To create btree index, please use 'CREATE BTREE INDEX ...'.""".stripMargin)
        }

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

  override def operation: String = "create fulltext index"
}

case object CreateTextIndexOptionsConverter extends IndexOptionsConverter[CreateProviderOnlyIndexOptions] {

  override def convert(options: MapValue): CreateProviderOnlyIndexOptions =  {
    val (maybeIndexProvider, _) = getOptionsParts(options, "text index")
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider)
    CreateProviderOnlyIndexOptions(indexProvider)
  }

  private def assertValidIndexProvider(indexProvider: AnyValue): IndexProviderDescriptor = indexProvider match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()

      if (indexProviderString.equalsIgnoreCase(FulltextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create text index with specified index provider '$indexProviderString'.
             |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin)

      if (indexProviderString.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) ||
        indexProviderString.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create text index with specified index provider '$indexProviderString'.
             |To create btree index, please use 'CREATE BTREE INDEX ...'.""".stripMargin)

      if (indexProviderString.equalsIgnoreCase(TokenIndexProvider.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create text index with specified index provider '$indexProviderString'.
             |To create token lookup index, please use 'CREATE LOOKUP INDEX ...'.""".stripMargin)

      if (!indexProviderString.equalsIgnoreCase(TextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create text index with specified index provider '$indexProviderString'.")

      TextIndexProviderFactory.DESCRIPTOR

    case _ =>
      throw new InvalidArgumentsException(s"Could not create text index with specified index provider '$indexProvider'. Expected String value.")
  }

  override def assertValidAndTransformConfig(config: AnyValue, schemaType: String): java.util.Map[String, Object] = {
    // for indexProvider TEXT: no available config settings
    val pp = new PrettyPrinter()
    config match {
      case itemsMap: MapValue =>
        if (itemsMap.exists { case (p, _) => p.equalsIgnoreCase(FULLTEXT_ANALYZER.getSettingName) || p.equalsIgnoreCase(FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName) }) {
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create text index with specified index config '${pp.value()}', contains fulltext config options.
               |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin)
        }

        if (itemsMap.exists { case (p: String, _) =>
          p.equalsIgnoreCase(SPATIAL_CARTESIAN_MIN.getSettingName) ||
            p.equalsIgnoreCase(SPATIAL_CARTESIAN_MAX.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MIN.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_CARTESIAN_3D_MAX.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_MIN.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_MAX.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_3D_MIN.getSettingName)  ||
            p.equalsIgnoreCase(SPATIAL_WGS84_3D_MAX.getSettingName)
        }) {
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create text index with specified index config '${pp.value()}', contains btree config options.
               |To create btree index, please use 'CREATE BTREE INDEX ...'.""".stripMargin)
        }

        if(!itemsMap.isEmpty){
          itemsMap.writeTo(pp)
          throw new InvalidArgumentsException(
            s"""Could not create text index with specified index config '${pp.value()}': text indexes have no valid config values.""".stripMargin)
        }

        Collections.emptyMap()
      case unknown =>
        unknown.writeTo(pp)
        throw new InvalidArgumentsException(s"Could not create text index with specified index config '${pp.value()}'. Expected a map.")
    }
  }

  override def operation: String = "create text index"
}

case class CreateWithNoOptions()
case class CreateBtreeIndexOptions(provider: Option[String], config: IndexConfig)
case class CreateProviderOnlyIndexOptions(provider: Option[IndexProviderDescriptor])
case class CreateFulltextIndexOptions(provider: Option[IndexProviderDescriptor], config: IndexConfig)
case class CreateDatabaseOptions(existingData: Option[String], databaseSeed: Option[String]) {
  def validate(dbName: String): Unit = {
    (existingData, databaseSeed) match {
      case (Some(_), None) | (None, Some(_)) =>
        throw new InvalidArgumentsException(s"Could not create database '$dbName'. Both existingData and databaseSeed options are required to seed database.")
      case _ => ()
    }
  }
}

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
