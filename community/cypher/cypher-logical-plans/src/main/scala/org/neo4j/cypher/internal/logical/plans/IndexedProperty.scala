/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.v4_0.expressions.{NODE_TYPE, CachedProperty, PropertyKeyName, PropertyKeyToken, Variable}
import org.neo4j.cypher.internal.v4_0.util.InputPosition

case class IndexedProperty(propertyKeyToken: PropertyKeyToken, getValueFromIndex: GetValueFromIndexBehavior) {
  def shouldGetValue: Boolean = getValueFromIndex == GetValue

  def asCachedProperty(node: String): CachedProperty =
    CachedProperty(node, Variable(node)(InputPosition.NONE), PropertyKeyName(propertyKeyToken.name)(InputPosition.NONE), NODE_TYPE)(InputPosition.NONE)

  def maybeCachedProperty(entity: String): Option[CachedProperty] =
    if (shouldGetValue)
      Some(asCachedProperty(entity))
    else None
}

// This can be extended later on with: GetValuesPartially
sealed trait GetValueFromIndexBehavior
case object DoNotGetValue extends GetValueFromIndexBehavior
case object CanGetValue extends GetValueFromIndexBehavior
case object GetValue extends GetValueFromIndexBehavior
