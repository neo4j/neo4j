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
package org.neo4j.cypher.internal.pipes

import collection.JavaConverters._

object MutableMaps {
  def empty = collection.mutable.Map[String, Any]()

  def create(size: Int) = new java.util.HashMap[String, Any](size).asScala

  def create(input: scala.collection.Map[String, Any]) = new java.util.HashMap[String, Any](input.asJava).asScala

  def create(input: (String, Any)*) = {
    val m: java.util.HashMap[String, Any] = new java.util.HashMap[String, Any]()
    input.foreach {
      case (k, v) => m.put(k, v)
    }
    m.asScala
  }
}