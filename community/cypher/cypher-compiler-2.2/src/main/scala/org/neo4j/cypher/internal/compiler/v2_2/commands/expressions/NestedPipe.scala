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
package org.neo4j.cypher.internal.compiler.v2_2.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

case class NestedPipe(pipe: Pipe, path: ProjectedPath) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    // this copy is needed to support in-place update by expand
    val innerState = state.copy(initialContext = Some(ctx.clone()))
    pipe.createResults(innerState).map(ctx => path(ctx)).toSeq
  }

  def rewrite(f: (Expression) => Expression) = f(this)

  def arguments = Nil

  def calculateType(symbols: SymbolTable): CypherType = CTCollection(CTPath)

  def symbolTableDependencies = Set()
}
