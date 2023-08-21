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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.values.storable.Value

/**
 * Cypher row which allows caching property values.
 */
trait CachedPropertiesRow {

  /**
   * Returns the cached property value
   *   or NO_VALUE if the entity does not have the property,
   *   or null     if this cached value has been invalidated, or the property value has not been cached.
   */
  def getCachedProperty(key: ASTCachedProperty.RuntimeKey): Value

  /**
   * Returns the cached property value
   *   or NO_VALUE if the entity does not have the property,
   *   or null     if this cached value has been invalidated, or the property value has not been cached.
   */
  def getCachedPropertyAt(offset: Int): Value

  def setCachedProperty(key: ASTCachedProperty.RuntimeKey, value: Value): Unit

  def setCachedPropertyAt(offset: Int, value: Value): Unit
}
