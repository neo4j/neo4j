/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.spi

trait TokenContext {
  def getLabelName(id: Int): String
  def getOptLabelId(labelName: String): Option[Int]
  def getLabelId(labelName: String): Int
  def getPropertyKeyName(id: Int): String
  def getOptPropertyKeyId(propertyKeyName: String): Option[Int]
  def getPropertyKeyId(propertyKeyName: String): Int
  def getRelTypeName(id: Int): String
  def getOptRelTypeId(relType: String): Option[Int]
  def getRelTypeId(relType: String): Int
}
