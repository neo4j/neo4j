/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_2.ParameterWrongTypeException

case class ToBooleanFunction(a: Expression) extends NullInNullOutExpression(a) {
  def symbolTableDependencies: Set[String] = a.symbolTableDependencies

  def arguments: Seq[Expression] = Seq(a)

  def rewrite(f: (Expression) => Expression): Expression = f(ToBooleanFunction(a.rewrite(f)))

  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = value match {
    case b: Boolean => b
    case v: String =>
      try {
        v.trim.toBoolean
      } catch {
        case e: IllegalArgumentException =>
          null
      }
    case v =>
      throw new ParameterWrongTypeException("Expected a String or Boolean, got: " + v.toString)
  }
}
