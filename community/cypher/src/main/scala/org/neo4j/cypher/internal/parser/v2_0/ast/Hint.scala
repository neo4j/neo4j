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
package org.neo4j.cypher.internal.parser.v2_0.ast

import org.neo4j.cypher.internal.parser.v2_0._
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.symbols._

sealed trait Hint extends AstNode with SemanticCheckable {
  def toLegacySchemaIndex : commands.StartItem with commands.Hint
}

case class UsingIndexHint(identifier: Identifier, label: Identifier, property: Identifier, token: InputToken) extends Hint {
  def semanticCheck = identifier.ensureDefined then identifier.constrainType(NodeType())

  def toLegacySchemaIndex = commands.SchemaIndex(identifier.name, label.name, property.name, commands.AnyIndex, None)
}

case class UsingScanHint(identifier: Identifier, label: Identifier, token: InputToken) extends Hint {
  def semanticCheck = identifier.ensureDefined then identifier.constrainType(NodeType())

  def toLegacySchemaIndex = commands.NodeByLabel(identifier.name, label.name)
}
