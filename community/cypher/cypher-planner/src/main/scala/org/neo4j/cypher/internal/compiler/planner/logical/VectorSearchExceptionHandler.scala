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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.planner.spi.VectorIndexError
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.VectorIndexSearchException
import org.neo4j.graphdb.schema.IndexType

import java.util.Locale

case object VectorSearchExceptionHandler {

  def handleErrors(indexDescriptorError: VectorIndexError, indexName: String, bindingVariableName: String) = {
    indexDescriptorError match {
      case VectorIndexError.NotFound =>
        throw VectorIndexSearchException.indexNotFound(indexName)
      case VectorIndexError.Populating =>
        throw VectorIndexSearchException.indexInPopulatingState(indexName)
      case VectorIndexError.WrongIndexType(wrongIndexType) =>
        throw InvalidArgumentException.wrongIndexType(
          indexName,
          IndexType.VECTOR.name().toLowerCase(Locale.ROOT), // must be a VECTOR index
          wrongIndexType.name().toLowerCase(Locale.ROOT) // the index type of the index name that was provided
        )
      case VectorIndexError.WrongEntityType(variableType, indexType) =>
        throw VectorIndexSearchException.wrongBindingVariableType(
          bindingVariableName,
          // the required type (for the binding variable) for the index name that was provided
          indexType.name(),
          // the actual type of the binding variable
          variableType.name()
        )
    }
  }
}
