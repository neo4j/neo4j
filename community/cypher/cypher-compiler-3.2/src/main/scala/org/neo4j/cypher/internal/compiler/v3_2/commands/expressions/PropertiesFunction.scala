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

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.helpers.IsMap
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_2.CypherTypeException

case class PropertiesFunction(a: Expression) extends NullInNullOutExpression(a) {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState) =
    value match {
      case IsMap(mapValue) => mapValue(state.query).toMap
      case v =>
        throw new CypherTypeException(s"Expected a Node, Relationship, or Map, got: $v")
    }

  override def arguments = Seq(a)

  override def rewrite(f: (Expression) => Expression) = f(PropertiesFunction(a.rewrite(f)))
}
