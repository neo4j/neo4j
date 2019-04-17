/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue

case class PropertiesFunction(a: Expression) extends NullInNullOutExpression(a) {
  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState) = CypherFunctions.properties(value, state.query)

  override def symbolTableDependencies: Set[String] = a.symbolTableDependencies

  override def arguments: Seq[Expression] = Seq(a)

  override def children: Seq[AstNode[_]] = Seq(a)

  override def rewrite(f: Expression => Expression): Expression = f(PropertiesFunction(a.rewrite(f)))
}
