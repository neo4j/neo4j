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
package org.neo4j.cypher.internal.compiler.v2_3.commands

sealed abstract class IndexOperation extends AbstractQuery {
  val label: String
}

// TODO use label: LabelValue?
final case class CreateIndex(label: String, propertyKeys: Seq[String], queryString: QueryString = QueryString.empty) extends IndexOperation {
  def setQueryText(t: String): CreateIndex = copy(queryString = QueryString(t))
}

final case class DropIndex(label: String, propertyKeys: Seq[String], queryString: QueryString = QueryString.empty) extends IndexOperation {
  def setQueryText(t: String): DropIndex = copy(queryString = QueryString(t))
}

sealed abstract class PropertyConstraintOperation extends AbstractQuery {
  def id: String
  def idForProperty: String
  def propertyKey: String
}

sealed abstract class NodePropertyConstraintOperation extends PropertyConstraintOperation {
  def label: String
}

sealed abstract class RelationshipPropertyConstraintOperation extends PropertyConstraintOperation {
  def relType: String
}

final case class CreateUniqueConstraint(id: String, label: String, idForProperty: String, propertyKey: String,
                                        queryString: QueryString = QueryString.empty) extends NodePropertyConstraintOperation {
  def setQueryText(t: String): CreateUniqueConstraint = copy(queryString = QueryString(t))
}

final case class DropUniqueConstraint(id: String, label: String, idForProperty: String, propertyKey: String,
                                      queryString: QueryString = QueryString.empty) extends NodePropertyConstraintOperation {
  def setQueryText(t: String): DropUniqueConstraint = copy(queryString = QueryString(t))
}

final case class CreateNodePropertyExistenceConstraint(id: String, label: String, idForProperty: String,
                                                       propertyKey: String, queryString: QueryString = QueryString.empty) extends NodePropertyConstraintOperation {
  def setQueryText(t: String): CreateNodePropertyExistenceConstraint = copy(queryString = QueryString(t))
}

final case class DropNodePropertyExistenceConstraint(id: String, label: String, idForProperty: String, propertyKey: String,
                                                     queryString: QueryString = QueryString.empty) extends NodePropertyConstraintOperation {
  def setQueryText(t: String): DropNodePropertyExistenceConstraint = copy(queryString = QueryString(t))
}

final case class CreateRelationshipPropertyExistenceConstraint(id: String, relType: String, idForProperty: String,
                                                               propertyKey: String, queryString: QueryString = QueryString.empty) extends RelationshipPropertyConstraintOperation {
  def setQueryText(t: String): CreateRelationshipPropertyExistenceConstraint = copy(queryString = QueryString(t))
}

final case class DropRelationshipPropertyExistenceConstraint(id: String, relType: String, idForProperty: String, propertyKey: String,
                                                             queryString: QueryString = QueryString.empty) extends RelationshipPropertyConstraintOperation {
  def setQueryText(t: String): DropRelationshipPropertyExistenceConstraint = copy(queryString = QueryString(t))
}
