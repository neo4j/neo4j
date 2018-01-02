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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

object MutableMaps {

  def create(size: Int) : collection.mutable.Map[String, Any] =
    new collection.mutable.OpenHashMap[String, Any](if (size < 16) 16 else size)

  def empty: collection.mutable.Map[String, Any] = create(16)

  def create(input: scala.collection.Map[String, Any]) : collection.mutable.Map[String, Any] =
    create(input.size) ++= input

  def create(input: (String, Any)*) : collection.mutable.Map[String, Any] = {
    create(input.size) ++= input
  }
}
