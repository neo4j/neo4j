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
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util.Collections
import scala.collection.JavaConverters.asScalaIteratorConverter

trait OptionsConverter[T] {

  val evaluator: ExpressionEvaluator = Evaluator.expressionEvaluator()

  def evaluate(expression: Expression, params: MapValue): AnyValue = {
      evaluator.evaluate(expression, params)
  }

  def convert(options: Map[String, Expression], params: MapValue = MapValue.EMPTY): T
}

case object CreateDatabaseOptionsConverter extends OptionsConverter[CreateDatabaseOptions] {
  val EXISTING_DATA = "existingData"
  val EXISTING_SEED_INSTANCE = "existingDataSeedInstance"
  val PERMITTED_OPTIONS = s"'$EXISTING_DATA', '$EXISTING_SEED_INSTANCE'"

  //existing Data values
  val USE_EXISTING_DATA = "use"

  def convert(e:AnyValue): CreateDatabaseOptions = {
    e match {
      case mv: MapValue => mapValueToCreateDatabaseOptions(mv)
      case _ =>
        throw new InvalidArgumentsException(s"Could not create database with options '$e'. Expected a map value.")
    }
  }

  def convert(options: Map[String, Expression], params: MapValue = MapValue.EMPTY): CreateDatabaseOptions = {
    val a = options.mapValues(evaluate(_, params))
    mapValueToCreateDatabaseOptions(VirtualValues.map(a.keys.toArray, a.values.toArray))
  }

  private def mapValueToCreateDatabaseOptions(map: MapValue): CreateDatabaseOptions = {
      var dbSeed: Option[String] = None
      map.foreach { case (key, value) =>
      //existingData
      if (key.equalsIgnoreCase(EXISTING_DATA)) {
        value match {
          case existingDataVal: TextValue if USE_EXISTING_DATA.equalsIgnoreCase(existingDataVal.stringValue()) =>
          case value: TextValue =>
            throw new InvalidArgumentsException(s"Could not create database with specified $EXISTING_DATA '${value.stringValue()}'. Expected '$USE_EXISTING_DATA'.")
          case value: AnyValue =>
            throw new InvalidArgumentsException(s"Could not create database with specified $EXISTING_DATA '$value'. Expected '$USE_EXISTING_DATA'.")
        }

        //existingDataSeedInstance
      } else if (key.equalsIgnoreCase(EXISTING_SEED_INSTANCE)) {
        value match {
          case seed: TextValue =>
            dbSeed = Some(seed.stringValue())
          case _ =>
            throw new InvalidArgumentsException(s"Could not create database with specified $EXISTING_SEED_INSTANCE '$value'. Expected database uuid string.")
        }
      } else {
        throw new InvalidArgumentsException(s"Could not create database with unrecognised option: '$key'. Expected $PERMITTED_OPTIONS.")
      }
    }
    CreateDatabaseOptions(USE_EXISTING_DATA, dbSeed)
  }
}

trait IndexOptionsConverter[T] extends OptionsConverter[T] {
  implicit class MapValueExists(mv: MapValue) {
    def exists(f: (Any, Any) => Boolean): Boolean = {
      var result = false
      mv.foreach { case (k, v) => if (f(k, v)) result = true }
      result
    }
  }

  def getOptionsParts(options: Map[String, Expression], params: MapValue, schemaType: String): (Option[Expression], IndexConfig) = {
    val lowerCaseOptions = options.map { case (k, v) => (k.toLowerCase, v) }
    val maybeIndexProvider = lowerCaseOptions.get("indexprovider")
    val maybeConfig = lowerCaseOptions.get("indexconfig")

    val configMap: java.util.Map[String, Object] = maybeConfig.map(assertValidAndTransformConfig(_, params, schemaType)).getOrElse(Collections.emptyMap())
    val indexConfig = IndexSettingUtil.toIndexConfigFromStringObjectMap(configMap)
    (maybeIndexProvider, indexConfig)
  }

  def assertValidAndTransformConfig(config: Expression, params: MapValue, schemaType: String): java.util.Map[String, Object]
}

case class CreateBtreeIndexOptionsConverter(schemaType: String) extends IndexOptionsConverter[CreateBtreeIndexOptions] {

  override def convert(options: Map[String, Expression], params: MapValue = MapValue.EMPTY): CreateBtreeIndexOptions =  {
    val (maybeIndexProvider, indexConfig) = getOptionsParts(options, params, schemaType)
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider(_, params, schemaType))
    CreateBtreeIndexOptions(indexProvider, indexConfig)
  }

  private def assertValidIndexProvider(indexProvider: Expression, params: MapValue, schemaType: String): String = evaluate(indexProvider, params) match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()
      if (indexProviderString.equalsIgnoreCase(FulltextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create $schemaType with specified index provider '$indexProviderString'.
             |To create fulltext index, please use 'CREATE FULLTEXT INDEX ...'.""".stripMargin)

      if (!indexProviderString.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) &&
        !indexProviderString.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderString'.")

      indexProviderString

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '${indexProvider.asCanonicalStringVal}'. Expected String value.")
  }

  override def assertValidAndTransformConfig(config: Expression, params: MapValue, schemaType: String): java.util.Map[String, Object] = {

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map from String to Double[].")
    }

    // for indexProvider BTREE:
    //    current keys: spatial.* (cartesian.|cartesian-3d.|wgs-84.|wgs-84-3d.) + (min|max)
    //    current values: Double[]
    evaluate(config, params) match {
      case itemsMap: MapValue =>
        if (itemsMap.exists { case (p: String, _) => p.equalsIgnoreCase(FULLTEXT_ANALYZER.getSettingName) || p.equalsIgnoreCase(FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName) }) {
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
}

case object CreateFulltextIndexOptionsConverter extends IndexOptionsConverter[CreateFulltextIndexOptions] {

  override def convert(options: Map[String, Expression], params: MapValue = MapValue.EMPTY): CreateFulltextIndexOptions =  {
    val (maybeIndexProvider, indexConfig) = getOptionsParts(options, params, "fulltext index")
    val indexProvider = maybeIndexProvider.map(assertValidIndexProvider(_, params))
    CreateFulltextIndexOptions(indexProvider, indexConfig)
  }

  private def assertValidIndexProvider(indexProvider: Expression, params: MapValue): IndexProviderDescriptor = evaluate(indexProvider, params) match {
    case indexProviderValue: TextValue =>
      val indexProviderString = indexProviderValue.stringValue()
      if (indexProviderString.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) ||
        indexProviderString.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create fulltext index with specified index provider '$indexProviderString'.
             |To create btree index, please use 'CREATE INDEX ...'.""".stripMargin)

      if (!indexProviderString.equalsIgnoreCase(FulltextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create fulltext index with specified index provider '$indexProviderString'.")

      FulltextIndexProviderFactory.DESCRIPTOR

    case _ =>
      throw new InvalidArgumentsException(s"Could not create fulltext index with specified index provider '${indexProvider.asCanonicalStringVal}'. Expected String value.")
  }

  override def assertValidAndTransformConfig(config: Expression, params: MapValue, schemaType: String): java.util.Map[String, Object] = {

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(s"Could not create fulltext index with specified index config '${pp.value()}'. Expected a map from String to Strings and Booleans.")
    }

    // for indexProvider FULLTEXT:
    //    current keys: fulltext.analyzer and fulltext.eventually_consistent
    //    current values: string and boolean
    evaluate(config, params) match {
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
               |To create btree index, please use 'CREATE INDEX ...'.""".stripMargin)
        }

        val hm = new java.util.HashMap[String, Object]()
        itemsMap.foreach {
          case (p: String, e: StringValue)  =>
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
}

case class CreateBtreeIndexOptions(provider: Option[String], config: IndexConfig)
case class CreateFulltextIndexOptions(provider: Option[IndexProviderDescriptor], config: IndexConfig)
case class CreateDatabaseOptions(existingData: String, databaseSeed: Option[String])
