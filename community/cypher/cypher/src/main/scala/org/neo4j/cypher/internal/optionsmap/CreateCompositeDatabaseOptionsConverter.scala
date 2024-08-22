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
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.values.virtual.MapValue

case object CreateCompositeDatabaseOptionsConverter extends OptionsConverter[CreateDatabaseOptions] {
  // Composite databases do not have any valid options, but allows for an empty options map

  override def convert(options: MapValue, config: Option[Config]): CreateDatabaseOptions = {
    if (!options.isEmpty)
      throw new InvalidArgumentsException(
        s"Could not $operation: composite databases have no valid options values."
      )
    CreateDatabaseOptions(None, None, None, None, None, None, None, None)
  }

  override def operation: String = s"create composite database"
}
