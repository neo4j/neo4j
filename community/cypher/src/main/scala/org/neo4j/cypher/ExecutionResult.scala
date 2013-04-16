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
package org.neo4j.cypher

import java.io.PrintWriter
import java.lang.String

trait ExecutionResult extends Iterator[Map[String, Any]] {
  def columns: List[String]
  def javaColumns: java.util.List[String]
  def javaColumnAs[T](column: String): java.util.Iterator[T]
  def columnAs[T](column: String): Iterator[T]
  def javaIterator: java.util.Iterator[java.util.Map[String, Any]]
  def dumpToString(writer: PrintWriter)
  def dumpToString(): String
  def queryStatistics(): QueryStatistics
  def executionPlanDescription(): PlanDescription
}

// Whenever you add a field here, please update the following classes:
//
// org.neo4j.cypher.javacompat.QueryStatistics
// org.neo4j.server.rest.repr.CypherResultRepresentation
// org.neo4j.server.rest.CypherFunctionalTest
//
case class QueryStatistics(nodesCreated: Int = 0,
                           relationshipsCreated: Int = 0,
                           propertiesSet: Int = 0,
                           deletedNodes: Int = 0,
                           deletedRelationships: Int = 0,
                           addedLabels: Int = 0,
                           removedLabels: Int = 0) {
  def containsUpdates = nodesCreated > 0 ||
  relationshipsCreated > 0 ||
  propertiesSet > 0 ||
  deletedNodes > 0 ||
  deletedRelationships > 0 ||
  addedLabels > 0 ||
  removedLabels > 0

  override def toString = {
    val builder = new StringBuilder

    includeIfNonZero(builder, "Nodes created: ", nodesCreated)
    includeIfNonZero(builder, "Relationships created: ", relationshipsCreated)
    includeIfNonZero(builder, "Properties set: ", propertiesSet)
    includeIfNonZero(builder, "Nodes deleted: ", deletedNodes)
    includeIfNonZero(builder, "Relationships deleted: ", deletedRelationships)
    includeIfNonZero(builder, "Labels added: ", addedLabels)
    includeIfNonZero(builder, "Labels removed: ", removedLabels)

    val result = builder.toString()

    if (result.isEmpty) "<Nothing happened>" else result
  }

  private def includeIfNonZero(builder:StringBuilder, message: String, count:Long) = if(count>0) {
    builder.append(message + count.toString + "\n")
  }
}

