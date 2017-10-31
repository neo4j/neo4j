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
package org.neo4j.cypher.internal.helpers

import org.neo4j.cypher.GraphIcing
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService

trait ConstraintCreator extends GraphIcing {
  def createConstraint(graph: GraphDatabaseCypherService, label: String, property: String)

  def typeName: String

  def other: ConstraintCreator
}

object UniquenessConstraintCreator extends ConstraintCreator {
  def createConstraint(graph: GraphDatabaseCypherService, label: String, property: String) =
    graph.createConstraint(label, property)

  override def toString = "Uniqueness Constraint"

  override def other = NodeKeyConstraintCreator

  def typeName = "UNIQUENESS"
}

object NodeKeyConstraintCreator extends ConstraintCreator {
  def createConstraint(graph: GraphDatabaseCypherService, label: String, property: String) =
    graph.createNodeKeyConstraint(label, property)

  override def toString = "NODE KEY Constraint"

  override def other = UniquenessConstraintCreator

  def typeName = "NODE_KEY"
}
