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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.symbols.{Typed, SymbolTable}
import pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.NotFoundException
import org.neo4j.helpers.ThisShouldNotHappenError

case class Identifier(entityName: String) extends Expression with Typed {

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any =
    ctx.getOrElse(entityName, throw new NotFoundException("Unknown identifier `%s`.".format(entityName)))

  override def toString: String = entityName

  def rewrite(f: (Expression) => Expression) = f(this)

  def arguments = Seq()

  def calculateType(symbols: SymbolTable) =
    throw new ThisShouldNotHappenError("Andres", "This class should override evaluateType, and this method should never be run")

  override def evaluateType(expectedType: CypherType, symbols: SymbolTable) = symbols.evaluateType(entityName, expectedType)

  def symbolTableDependencies = Set(entityName)
}
