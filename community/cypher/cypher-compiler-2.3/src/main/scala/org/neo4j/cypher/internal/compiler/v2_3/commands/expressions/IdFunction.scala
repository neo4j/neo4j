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
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, Relationship}

case class IdFunction(inner: Expression) extends NullInNullOutExpression(inner) {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState) = value match {
    case node: Node        => node.getId
    case rel: Relationship => rel.getId
    case x => throw new CypherTypeException("Expected `%s` to be a node or relationship, but it was ``".format(inner, x.getClass.getSimpleName))
  }

  def rewrite(f: (Expression) => Expression) = f(IdFunction(inner.rewrite(f)))

  def arguments = Seq(inner)

  def calculateType(symbols: SymbolTable): CypherType = CTInteger

  def symbolTableDependencies = inner.symbolTableDependencies
}
