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
import org.neo4j.cypher.internal.MapValueOps.Ops
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.AnyValue
import org.neo4j.values.SequenceValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.MapValue

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

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
  ): IndexConfig = {
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
      case itemsMap: MapValue if itemsMap.isEmpty => IndexConfig.empty
      case itemsMap: MapValue =>
        checkForFulltextConfigValues(new PrettyPrinter(), itemsMap, schemaType)
        checkForVectorConfigValues(new PrettyPrinter(), itemsMap, schemaType)

        val map = itemsMap.foldLeft(Map[String, Object]()) {
          // Allow both list and array
          case (m, (p: String, e: SequenceValue)) =>
            val configValue: Array[Double] = e.iterator().asScala.map {
              // Allow all numbers, and convert them to double
              case d: NumberValue => d.doubleValue()
              case _              => throw exceptionWrongType(itemsMap)
            }.toArray
            m + (p -> configValue)
          case _ => throw exceptionWrongType(itemsMap)
        }.asJava

        toIndexConfig(map)
      case unknown =>
        throw exceptionWrongType(unknown)
    }
  }

  override def operation: String = s"create $schemaType"
}
