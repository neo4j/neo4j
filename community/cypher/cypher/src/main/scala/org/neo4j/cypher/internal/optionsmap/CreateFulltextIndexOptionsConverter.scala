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
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.utils.PrettyPrinter
import org.neo4j.values.virtual.MapValue

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
  ): IndexConfig = {

    def exceptionWrongType(suppliedValue: AnyValue): InvalidArgumentsException = {
      val pp = new PrettyPrinter()
      suppliedValue.writeTo(pp)
      new InvalidArgumentsException(
        s"Could not create $schemaType with specified index config '${pp.value()}'. Expected a map from String to Strings and Booleans."
      )
    }

    config match {
      case itemsMap: MapValue if itemsMap.isEmpty => IndexConfig.empty
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

        toIndexConfig(hm)
      case unknown =>
        throw exceptionWrongType(unknown)
    }
  }

  override def operation: String = s"create $schemaType"
}
