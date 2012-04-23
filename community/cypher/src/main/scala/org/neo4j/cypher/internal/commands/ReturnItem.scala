/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.cypher.internal.pipes.Dependant
import org.neo4j.cypher.internal.symbols._

abstract class ReturnColumn extends Dependant {
  def expressions(symbols: SymbolTable): Seq[Expression]

  def name: String
}

case class AllIdentifiers() extends ReturnColumn {
  def expressions(symbols: SymbolTable) = symbols.identifiers.flatMap {
    case Identifier(name, _) if name.startsWith("  UNNAMED") => None
    case Identifier(name, typ) if MapType().isAssignableFrom(typ) => Some(Entity(name))
    case Identifier(name, typ) if PathType().isAssignableFrom(typ) => Some(Entity(name))
    case _ => None
  }

  def name = "*"

  def dependencies = Seq()
}

case class ReturnItem(expression: Expression, name: String, renamed: Boolean = false) extends ReturnColumn {
  def expressions(symbols: SymbolTable) = Seq(expression)

  def dependencies = expression.dependencies(AnyType())

  def identifier = Identifier(name, expression.identifier.typ)

  def columnName = identifier.name

  override def toString() = identifier.name

  def rename(newName: String) = ReturnItem(expression, newName, true)
}