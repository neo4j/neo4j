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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.symbols.{SymbolTable, CypherType}
import org.neo4j.cypher.EntityNotFoundException
import collection.Map
import org.neo4j.cypher.internal.pipes.ExecutionContext
import org.neo4j.graphdb.NotFoundException

case class Nullable(expression: Expression) extends Expression {
  def apply(ctx: ExecutionContext) = try {
    expression.apply(ctx)
  } catch {
    case x: EntityNotFoundException => null
    case x: NotFoundException       => null
  }

  def rewrite(f: (Expression) => Expression) = f(Nullable(expression.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ expression.filter(f)
  else
    expression.filter(f)

  def calculateType(symbols: SymbolTable): CypherType = expression.getType(symbols)

  def symbolTableDependencies = expression.symbolTableDependencies
}