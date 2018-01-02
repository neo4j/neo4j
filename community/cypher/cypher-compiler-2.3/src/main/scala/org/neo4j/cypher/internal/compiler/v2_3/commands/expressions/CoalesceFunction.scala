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
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class CoalesceFunction(arguments: Expression*) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any =
    arguments.
      view.
      map(expression => expression(ctx)).
      find(value => value != null) match {
        case None    => null
        case Some(x) => x
      }

  def innerExpectedType: Option[CypherType] = None

  val argumentsString: String = children.mkString(",")

  override def toString = "coalesce(" + argumentsString + ")"

  def rewrite(f: (Expression) => Expression) = f(CoalesceFunction(arguments.map(e => e.rewrite(f)): _*))

  def calculateType(symbols: SymbolTable) = {
    arguments.map(_.getType(symbols)) match {
      case Seq() => CTAny
      case types => types.reduceLeft(_ leastUpperBound _)
    }
  }

  def symbolTableDependencies = arguments.flatMap(_.symbolTableDependencies).toSet
}
