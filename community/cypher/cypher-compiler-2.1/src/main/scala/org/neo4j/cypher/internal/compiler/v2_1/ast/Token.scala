/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.RelTypeId

trait Token[T <: TokenId] extends ASTNode with SymbolicName {
  def id: Option[T]
}

case class LabelToken(name: String, id: Option[LabelId] = None)(val position: InputPosition) extends Token[LabelId]
case class PropertyKeyToken(name: String, id: Option[PropertyKeyId] = None)(val position: InputPosition) extends Token[PropertyKeyId]
case class RelTypeToken(name: String, id: Option[RelTypeId] = None)(val position: InputPosition) extends Token[RelTypeId]

object LabelToken {
  def fromIdentifier(identifier: Identifier): LabelToken = LabelToken(identifier.name)(identifier.position)
}

object PropertyKeyToken {
  def fromIdentifier(identifier: Identifier): PropertyKeyToken = PropertyKeyToken(identifier.name)(identifier.position)
}

object RelTypeToken {
  def fromIdentifier(identifier: Identifier): RelTypeToken = RelTypeToken(identifier.name)(identifier.position)
}
