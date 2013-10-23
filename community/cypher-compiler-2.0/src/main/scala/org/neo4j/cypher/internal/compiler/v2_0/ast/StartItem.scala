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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.symbols.{NodeType, RelationshipType}
import org.neo4j.cypher.internal.compiler.v2_0.commands
import org.neo4j.cypher.internal.compiler.v2_0.commands.{expressions => commandexpressions}

sealed trait StartItem extends AstNode with SemanticCheckable {
  def identifier : Identifier

  def toCommand : commands.StartItem
}

sealed trait NodeStartItem extends StartItem {
  def semanticCheck = identifier.declare(NodeType())
}

case class NodeByIds(identifier: Identifier, ids: Seq[UnsignedInteger], token: InputToken) extends NodeStartItem {
  def toCommand = commands.NodeById(identifier.name, commandexpressions.Literal(ids.map(_.value)))
}

case class NodeByParameter(identifier: Identifier, parameter: Parameter, token: InputToken) extends NodeStartItem {
  def toCommand = commands.NodeById(identifier.name, parameter.toCommand)
}

case class AllNodes(identifier: Identifier, token: InputToken) extends NodeStartItem {
  def toCommand = commands.AllNodes(identifier.name)
}

sealed trait NodeByIndex extends NodeStartItem {
  def index: Identifier
}

case class NodeByIdentifiedIndex(identifier: Identifier, index: Identifier, key: Identifier, value: Expression, token: InputToken) extends NodeByIndex {
  def toCommand = commands.NodeByIndex(identifier.name, index.name, commandexpressions.Literal(key.name), value.toCommand)
}

case class NodeByIndexQuery(identifier: Identifier, index: Identifier, query: Expression, token: InputToken) extends NodeByIndex {
  def toCommand = commands.NodeByIndexQuery(identifier.name, index.name, query.toCommand)
}

sealed trait RelationshipStartItem extends StartItem {
  def semanticCheck = identifier.declare(RelationshipType())
}

case class RelationshipByIds(identifier: Identifier, ids: Seq[UnsignedInteger], token: InputToken) extends RelationshipStartItem {
  def toCommand = commands.RelationshipById(identifier.name, commandexpressions.Literal(ids.map(_.value)))
}

case class RelationshipByParameter(identifier: Identifier, parameter: Parameter, token: InputToken) extends RelationshipStartItem {
  def toCommand = commands.RelationshipById(identifier.name, parameter.toCommand)
}

case class AllRelationships(identifier: Identifier, token: InputToken) extends RelationshipStartItem {
  def toCommand = commands.AllRelationships(identifier.name)
}

sealed trait RelationshipByIndex extends RelationshipStartItem {
  def index: Identifier
}

case class RelationshipByIdentifiedIndex(identifier: Identifier, index: Identifier, key: Identifier, value: Expression, token: InputToken) extends RelationshipByIndex {
  def toCommand = commands.RelationshipByIndex(identifier.name, index.name, commandexpressions.Literal(key.name), value.toCommand)
}

case class RelationshipByIndexQuery(identifier: Identifier, index: Identifier, query: Expression, token: InputToken) extends RelationshipByIndex {
  def toCommand = commands.RelationshipByIndexQuery(identifier.name, index.name, query.toCommand)
}
