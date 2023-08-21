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
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression

trait SlottedCachedProperty extends ASTCachedProperty with RuntimeExpression {
  def offset: Int
  def offsetIsForLongSlot: Boolean
  def cachedPropertyOffset: Int
  def nullable: Boolean

  def needsValue: Boolean = true

  /**
   * For slotted we don't need to distinct between the original and rewritten name
   */
  override def originalEntityName: String = entityName
}

trait SlottedCachedHasProperty extends SlottedCachedProperty {
  override def needsValue: Boolean = false
}

/**
  *
  * @param entityName The name of the node or relationship
  * @param propertyKey the name of the property key
  * @param offset the offset into the long slots or ref slots for the entity
  * @param offsetIsForLongSlot if this is `true`, `offset` refers to the long slots. If this is `false`, `offset` refers to the ref slots.
  * @param propToken token for the cached property
  * @param cachedPropertyOffset offset of the cached property in the ref slots
  * @param entityType the type of the entity, node or relationship
  * @param nullable `true` if entity is nullable otherwise `false`
  */
case class SlottedCachedPropertyWithPropertyToken(
  entityName: String,
  propertyKey: PropertyKeyName,
  offset: Int,
  offsetIsForLongSlot: Boolean,
  propToken: Int,
  cachedPropertyOffset: Int,
  entityType: EntityType,
  nullable: Boolean
) extends SlottedCachedProperty

/**
 *
 * @param entityName The name of the node or relationship
 * @param propertyKey the name of the property key
 * @param offset the offset into the long slots or ref slots for the entity
 * @param offsetIsForLongSlot if this is `true`, `offset` refers to the long slots. If this is `false`, `offset` refers to the ref slots.
 * @param propToken token for the cached property
 * @param cachedPropertyOffset offset of the cached property in the ref slots
 * @param entityType the type of the entity, node or relationship
 * @param nullable `true` if entity is nullable otherwise `false`
 */
case class SlottedCachedHasPropertyWithPropertyToken(
  entityName: String,
  propertyKey: PropertyKeyName,
  offset: Int,
  offsetIsForLongSlot: Boolean,
  propToken: Int,
  cachedPropertyOffset: Int,
  entityType: EntityType,
  nullable: Boolean
) extends SlottedCachedHasProperty

object SlottedCachedPropertyWithPropertyToken {

  def apply(
    entityName: String,
    propertyKey: PropertyKeyName,
    offset: Int,
    offsetIsForLongSlot: Boolean,
    propToken: Int,
    cachedPropertyOffset: Int,
    entityType: EntityType,
    nullable: Boolean,
    needsValue: Boolean
  ): ASTCachedProperty = {
    if (needsValue) {
      SlottedCachedPropertyWithPropertyToken(
        entityName,
        propertyKey,
        offset,
        offsetIsForLongSlot,
        propToken,
        cachedPropertyOffset,
        entityType,
        nullable
      )
    } else {
      SlottedCachedHasPropertyWithPropertyToken(
        entityName,
        propertyKey,
        offset,
        offsetIsForLongSlot,
        propToken,
        cachedPropertyOffset,
        entityType,
        nullable
      )
    }
  }
}

// Token did not exist at plan time, so we'll need to look it up at runtime
/**
  *
  * @param entityName The name of the node or relationship
  * @param propertyKey the name of the property key
  * @param offset the offset into the long slots or ref slots for the entity
  * @param offsetIsForLongSlot if this is `true`, `offset` refers to the long slots. If this is `false`, `offset` refers to the ref slots.
  * @param propKey property name for the cached property
  * @param cachedPropertyOffset ffset of the cached property in the ref slots
  * @param entityType the type of the entity, node or relationship
  * @param nullable `true` if entity is nullable otherwise `false`
  */
case class SlottedCachedPropertyWithoutPropertyToken(
  entityName: String,
  propertyKey: PropertyKeyName,
  offset: Int,
  offsetIsForLongSlot: Boolean,
  propKey: String,
  cachedPropertyOffset: Int,
  entityType: EntityType,
  nullable: Boolean
) extends SlottedCachedProperty

/**
 *
 * @param entityName The name of the node or relationship
 * @param propertyKey the name of the property key
 * @param offset the offset into the long slots or ref slots for the entity
 * @param offsetIsForLongSlot if this is `true`, `offset` refers to the long slots. If this is `false`, `offset` refers to the ref slots.
 * @param propKey property name for the cached property
 * @param cachedPropertyOffset ffset of the cached property in the ref slots
 * @param entityType the type of the entity, node or relationship
 * @param nullable `true` if entity is nullable otherwise `false`
 */
case class SlottedCachedHasPropertyWithoutPropertyToken(
  entityName: String,
  propertyKey: PropertyKeyName,
  offset: Int,
  offsetIsForLongSlot: Boolean,
  propKey: String,
  cachedPropertyOffset: Int,
  entityType: EntityType,
  nullable: Boolean
) extends SlottedCachedHasProperty

object SlottedCachedPropertyWithoutPropertyToken {

  def apply(
    entityName: String,
    propertyKey: PropertyKeyName,
    offset: Int,
    offsetIsForLongSlot: Boolean,
    propKey: String,
    cachedPropertyOffset: Int,
    entityType: EntityType,
    nullable: Boolean,
    needsValue: Boolean
  ): ASTCachedProperty = {
    if (needsValue) {
      SlottedCachedPropertyWithoutPropertyToken(
        entityName,
        propertyKey,
        offset,
        offsetIsForLongSlot,
        propKey,
        cachedPropertyOffset,
        entityType,
        nullable
      )
    } else {
      SlottedCachedHasPropertyWithoutPropertyToken(
        entityName,
        propertyKey,
        offset,
        offsetIsForLongSlot,
        propKey,
        cachedPropertyOffset,
        entityType,
        nullable
      )
    }
  }
}
