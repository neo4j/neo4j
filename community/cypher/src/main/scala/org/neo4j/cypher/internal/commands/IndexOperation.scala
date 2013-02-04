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
package org.neo4j.cypher.internal.commands


sealed abstract class IndexOperation extends AbstractQuery

case class CreateIndex(label: String, properties: Seq[String], queryString: String = "") extends IndexOperation {
  def setQueryString(t: String): CreateIndex = copy(queryString = t)

  override def equals(p1: Any): Boolean = p1 match {
    case null               => false
    case other: CreateIndex => label == other.label && properties == other.properties
    case _                  => false
  }

}

case class DeleteIndex(label: String, properties: Seq[String], queryString: String = "") extends IndexOperation {
  def setQueryString(t: String): DeleteIndex = copy(queryString = t)

  override def equals(p1: Any): Boolean = p1 match {
    case null             => false
    case other: DeleteIndex => label == other.label && properties == other.properties
    case _                => false
  }

}