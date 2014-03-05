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

sealed trait SymbolicToken[T <: NameId]  {
  self: SymbolicName =>
  
  def id: Option[T]
}

final case class LabelName(name: String, id: Option[LabelId] = None)(val position: InputPosition)
  extends ASTNode with SymbolicName with SymbolicToken[LabelId]

final case class PropertyKeyName(name: String, id: Option[PropertyKeyId] = None)(val position: InputPosition)
  extends ASTNode with SymbolicName with SymbolicToken[PropertyKeyId]

final case class RelTypeName(name: String, id: Option[RelTypeId] = None)(val position: InputPosition)
  extends ASTNode with SymbolicName with SymbolicToken[RelTypeId]
