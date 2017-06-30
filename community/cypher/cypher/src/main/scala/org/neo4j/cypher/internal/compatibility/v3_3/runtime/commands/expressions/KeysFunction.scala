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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.IsMap
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.values.AnyValue

case class KeysFunction(expr: Expression) extends NullInNullOutExpression(expr) {

  override def compute(value: AnyValue, ctx: ExecutionContext)(implicit state: QueryState) = value match {
    case IsMap(map) => map.keys

    case x =>
      throw new CypherTypeException(s"Expected $expr to be a node, a relationship, or a literal map, but it was ${x.getClass.getSimpleName}")
  }

  def rewrite(f: (Expression) => Expression) = f(KeysFunction(expr.rewrite(f)))

  def arguments = Seq(expr)

  def symbolTableDependencies = expr.symbolTableDependencies
}
