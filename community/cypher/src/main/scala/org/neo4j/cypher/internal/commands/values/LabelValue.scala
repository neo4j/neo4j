/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.commands.values

import org.neo4j.cypher.internal.spi.QueryContext

sealed abstract class LabelValue {
  def resolveForName(ctx: QueryContext): LabelValue with LabelWithName
  def resolveForId(ctx: QueryContext): LabelValue with LabelWithId

  def resolveJavaId(ctx: QueryContext) = resolveForId(ctx).javaId
}

trait LabelWithId {
  def id: Long

  def javaId: java.lang.Long = id.asInstanceOf[java.lang.Long]
}

trait LabelWithName {
  def name: String
}

case class LabelName(name: String) extends LabelValue with LabelWithName {
  def resolveForName(ctx: QueryContext) = this
  def resolveForId(ctx: QueryContext) = ResolvedLabel(ctx.getOrCreateLabelId(name), name)
}

case class LabelId(id: Long) extends LabelValue with LabelWithId {
  def resolveForId(ctx: QueryContext) = this
  def resolveForName(ctx: QueryContext) = ResolvedLabel(id, ctx.getLabelName(id.asInstanceOf[java.lang.Long]))
}

case class ResolvedLabel(id: Long, name: String) extends LabelValue with LabelWithName with LabelWithId {
  def resolveForName(ctx: QueryContext) = this
  def resolveForId(ctx: QueryContext) = this
}