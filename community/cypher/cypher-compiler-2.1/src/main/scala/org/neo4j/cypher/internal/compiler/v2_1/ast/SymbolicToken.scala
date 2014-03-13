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
import org.neo4j.cypher.internal.compiler.v2_1.RelTypeId

sealed trait SymbolicToken[T <: NameId]  {
  self: SymbolicName =>

  def id: Option[T]

  protected def updatedId(newId: T): T = id match {
    case Some(oldId) => throw new IllegalStateException("Attempt to update already set id")
    case None        => newId
  }
}

final case class LabelName(name: String)(val id: Option[LabelId] = None)(val position: InputPosition)
  extends ASTNode with SymbolicName with SymbolicToken[LabelId] {

  def withId(newId: LabelId) = copy()(id = Some(updatedId(newId)))(position)
}

final case class PropertyKeyName(name: String)(val id: Option[PropertyKeyId] = None)(val position: InputPosition)
  extends ASTNode with SymbolicName with SymbolicToken[PropertyKeyId] {

  def withId(newId: PropertyKeyId) = copy()(id = Some(updatedId(newId)))(position)
}

final case class RelTypeName(name: String)(val id: Option[RelTypeId] = None)(val position: InputPosition)
  extends ASTNode with SymbolicName with SymbolicToken[RelTypeId] {

  def withId(newId: RelTypeId) = copy()(id = Some(updatedId(newId)))(position)
}
