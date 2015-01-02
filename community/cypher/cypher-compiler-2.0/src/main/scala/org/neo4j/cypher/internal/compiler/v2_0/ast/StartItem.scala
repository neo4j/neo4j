/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import symbols._

sealed trait StartItem extends ASTNode with SemanticCheckable {
  def identifier: Identifier
}


sealed trait NodeStartItem extends StartItem {
  def semanticCheck = identifier.declare(CTNode)
}

case class NodeByIds(identifier: Identifier, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition) extends NodeStartItem
case class NodeByParameter(identifier: Identifier, parameter: Parameter)(val position: InputPosition) extends NodeStartItem
case class AllNodes(identifier: Identifier)(val position: InputPosition) extends NodeStartItem
case class NodeByIdentifiedIndex(identifier: Identifier, index: Identifier, key: Identifier, value: Expression)(val position: InputPosition) extends NodeStartItem
case class NodeByIndexQuery(identifier: Identifier, index: Identifier, query: Expression)(val position: InputPosition) extends NodeStartItem


sealed trait RelationshipStartItem extends StartItem {
  def semanticCheck = identifier.declare(CTRelationship)
}

case class RelationshipByIds(identifier: Identifier, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition) extends RelationshipStartItem
case class RelationshipByParameter(identifier: Identifier, parameter: Parameter)(val position: InputPosition) extends RelationshipStartItem
case class AllRelationships(identifier: Identifier)(val position: InputPosition) extends RelationshipStartItem
case class RelationshipByIdentifiedIndex(identifier: Identifier, index: Identifier, key: Identifier, value: Expression)(val position: InputPosition) extends RelationshipStartItem
case class RelationshipByIndexQuery(identifier: Identifier, index: Identifier, query: Expression)(val position: InputPosition) extends RelationshipStartItem
