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
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

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
  ): IndexConfig =
    assertEmptyConfig(config, schemaType, "text")

  override def operation: String = s"create $schemaType"
}
