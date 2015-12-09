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
package org.neo4j.cypher.internal.compiler.v3_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_0.symbols._

case class ParameterExpression(parameterName: String) extends Expression {

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = state.getParam(parameterName) match {
    // this runtime check is necessary to enforce that Cypher never outputs a NaN
    case d: Double if java.lang.Double.isNaN(d) => null
    case f: Float if java.lang.Double.isNaN(f) => null
    case other => other
  }

  override def toString(): String = "{" + parameterName + "}"

  def rewrite(f: (Expression) => Expression) = f(this)

  def arguments = Seq()

  def calculateType(symbols: SymbolTable) = CTAny

  def symbolTableDependencies = Set()
}
