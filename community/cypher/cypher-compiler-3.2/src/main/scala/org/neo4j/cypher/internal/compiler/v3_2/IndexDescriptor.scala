/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2

import org.neo4j.cypher.internal.frontend.v3_2.{LabelId, PropertyKeyId}

object IndexDescriptor {
  def apply(label: Int, property: Int): IndexDescriptor = IndexDescriptor(LabelId(label), Seq(PropertyKeyId(property)))

  def apply(label: Int, properties: Seq[Int]): IndexDescriptor = IndexDescriptor(LabelId(label), properties.toSeq.map(PropertyKeyId))

  def apply(label: LabelId, property: PropertyKeyId): IndexDescriptor = IndexDescriptor(label, Seq(property))

  implicit def toKernelEncode(properties: Seq[PropertyKeyId]): Array[Int] = properties.map(_.id).toArray
}

case class IndexDescriptor(label: LabelId, properties: Seq[PropertyKeyId]) {
  def this(label: Int, property: Int) = this( LabelId(label), Array(PropertyKeyId(property)) )

  def isComposite: Boolean = properties.length > 1

  def property: PropertyKeyId = if (isComposite) throw new IllegalArgumentException("Cannot get single property of multi-property index") else properties(0)
}
