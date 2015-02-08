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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.QueryState
import symbols._

object Collection {
  val empty = Literal(Seq())
}

case class Collection(arguments: Expression*) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = arguments.map(e => e(ctx))

  def rewrite(f: (Expression) => Expression): Expression = f(Collection(arguments.map(f): _*))

  def calculateType(symbols: SymbolTable): CypherType =
    arguments.map(_.getType(symbols)) match {
      case Seq() => CTCollection(CTAny)
      case types => CTCollection(types.reduce(_ mergeUp _))
    }

  def symbolTableDependencies = arguments.flatMap(_.symbolTableDependencies).toSet
}
