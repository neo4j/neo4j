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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedRelationshipHasProperty
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedRelationshipProperty
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.VirtualRelationshipValue

case class SlottedCachedRelationshipProperty(
  relationshipOffset: Int,
  offsetIsForLongSlot: Boolean,
  propertyKey: Int,
  cachedPropertyOffset: Int
) extends AbstractCachedRelationshipProperty with SlottedExpression {

  override def getId(ctx: ReadableRow): Long =
    if (offsetIsForLongSlot)
      ctx.getLongAt(relationshipOffset)
    else
      ctx.getRefAt(relationshipOffset).asInstanceOf[VirtualRelationshipValue].id()

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit =
    ctx.setCachedPropertyAt(cachedPropertyOffset, value)

  override def getPropertyKey(tokenContext: ReadTokenContext): Int = propertyKey

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class SlottedCachedRelationshipPropertyLate(
  relationshipOffset: Int,
  offsetIsForLongSlot: Boolean,
  propertyKey: String,
  cachedPropertyOffset: Int
) extends AbstractCachedRelationshipProperty with SlottedExpression {

  override def getId(ctx: ReadableRow): Long =
    if (offsetIsForLongSlot)
      ctx.getLongAt(relationshipOffset)
    else
      ctx.getRefAt(relationshipOffset).asInstanceOf[VirtualRelationshipValue].id()

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit =
    ctx.setCachedPropertyAt(cachedPropertyOffset, value)

  override def getPropertyKey(tokenContext: ReadTokenContext): Int =
    tokenContext.getOptPropertyKeyId(propertyKey).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class SlottedCachedRelationshipHasProperty(
  relationshipOffset: Int,
  offsetIsForLongSlot: Boolean,
  propertyKey: Int,
  cachedPropertyOffset: Int
) extends AbstractCachedRelationshipHasProperty with SlottedExpression {

  override def getId(ctx: ReadableRow): Long =
    if (offsetIsForLongSlot)
      ctx.getLongAt(relationshipOffset)
    else
      ctx.getRefAt(relationshipOffset).asInstanceOf[VirtualRelationshipValue].id()

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit =
    ctx.setCachedPropertyAt(cachedPropertyOffset, value)

  override def getPropertyKey(tokenContext: ReadTokenContext): Int = propertyKey

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class SlottedCachedRelationshipHasPropertyLate(
  relationshipOffset: Int,
  offsetIsForLongSlot: Boolean,
  propertyKey: String,
  cachedPropertyOffset: Int
) extends AbstractCachedRelationshipHasProperty with SlottedExpression {

  override def getId(ctx: ReadableRow): Long =
    if (offsetIsForLongSlot)
      ctx.getLongAt(relationshipOffset)
    else
      ctx.getRefAt(relationshipOffset).asInstanceOf[VirtualRelationshipValue].id()

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit =
    ctx.setCachedPropertyAt(cachedPropertyOffset, value)

  override def getPropertyKey(tokenContext: ReadTokenContext): Int =
    tokenContext.getOptPropertyKeyId(propertyKey).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq.empty
}
