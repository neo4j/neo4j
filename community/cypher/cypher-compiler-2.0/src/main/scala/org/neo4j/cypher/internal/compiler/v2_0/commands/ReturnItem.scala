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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import expressions.{Identifier, Expression}
import expressions.Identifier._
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import collection.Map

abstract class ReturnColumn {
  def expressions(symbols: SymbolTable): Map[String,Expression]

  def name: String
}

case class AllIdentifiers() extends ReturnColumn {
  def expressions(symbols: SymbolTable): Map[String, Expression] = symbols.identifiers.keys.
    filter(isNamed).
    map(n => n -> Identifier(n)).toMap

  def name = "*"
}

case class ReturnItem(expression: Expression, name: String, renamed: Boolean = false)
  extends ReturnColumn {
  def expressions(symbols: SymbolTable) = Map(name -> expression)

  override def toString = if(renamed)
    s"${expression.toString} AS ${name}"
  else
    name

  def rename(newName: String) = ReturnItem(expression, newName, renamed = true)

  def map(f: Expression => Expression): ReturnItem = copy(expression = f(expression))
}