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
package org.neo4j.cypher.internal.spi

import org.neo4j.kernel.api.exceptions.KernelException

trait TokenContext {
  def getLabelName(id: Long): String

  def getOptLabelId(labelName: String): Option[Long]

  def getLabelId(labelName: String): Long

  def getPropertyKeyName(id: Long): String

  def getOptPropertyKeyId(propertyKeyName: String): Option[Long]

  def getPropertyKeyId(propertyKeyName: String): Long
}

object TokenContext
{
  def tryGet[T <: KernelException : Manifest](result: => Long) = try { Some(result) } catch { case (_: T) => None }
}