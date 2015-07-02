/*
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols._

sealed trait Extremum extends Expression {

  def expressions: Seq[Expression]

  def arguments: Seq[Expression] = expressions

  def calculateType(symbols: SymbolTable) =
    calculateUpperTypeBound(CTNumber, symbols, expressions)

  def symbolTableDependencies =
    expressions.flatMap(_.symbolTableDependencies).toSet

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    expressions.map(expr => expr(ctx)).reduce { (l, r) => fuse(l, r) }
  }

  def fuse(l: Any, r: Any): Any
}

final case class Minimum(expressions: Seq[Expression]) extends Extremum {

  def fuse(l: Any, r: Any): Any = ???

  override def rewrite(f: (Expression) => Expression): Expression = f(copy(expressions = expressions.map(f)))
}

final case class Maximum(expressions: Seq[Expression]) extends Extremum {

  def fuse(l: Any, r: Any): Any = ???

  override def rewrite(f: (Expression) => Expression): Expression = f(copy(expressions = expressions.map(f)))
}
