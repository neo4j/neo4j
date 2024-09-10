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
import org.neo4j.cypher.internal.runtime.IndexProviderContext
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

case class PropertyExistenceOrTypeConstraintOptionsConverter(
  entity: String,
  constraintType: String,
  context: IndexProviderContext
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
  ): IndexConfig = IndexConfig.empty

  override def operation: String = s"create $entity property $constraintType constraint"
}
